package grace

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	LISTEN_FD_NUM = "LISTEN_FDS"
)

//type Application interface {
//	Startup(l *net.Listener) error
//	Shutdown(ctx context.Context) error
//}

type filer interface {
	File() (*os.File, error)
}

// 比较两个地址是否相等
func compare(na1, na2 net.Addr) bool {
	// 网络类型不同直接返回false, 例如tcp与udp
	if na1.Network() != na2.Network() {
		return false
	}

	// 去除IPv4和IPv6前缀然后做比较
	const IPV4_PREFIX = "0.0.0.0"
	const IPV6_PREFIX = "[::]"
	na1s := strings.TrimPrefix(strings.TrimPrefix(na1.String(), IPV6_PREFIX), IPV4_PREFIX)
	na2s := strings.TrimPrefix(strings.TrimPrefix(na2.String(), IPV6_PREFIX), IPV4_PREFIX)

	return na1s == na2s
}

// 获取绝对路径
func which(cmd string) (string, error) {
	cmdPath, err := exec.LookPath(cmd)
	if err != nil {
		return cmd, err
	}
	filePath, err := filepath.Abs(cmdPath)
	if err != nil {
		return cmdPath, err
	}
	return filePath, nil
}

type Net struct {
	// 从父进程继承的监听句柄
	inherited []net.Listener
	// 互斥锁
	mutex sync.Mutex
	// 确保只从父进程继承一次监听句柄
	inheritOnce sync.Once
	// 当前进程活跃的监听句柄
	active []net.Listener
}

func (n *Net) inherit() (retErr error) {
	// 尝试从旧进程中继承连接句柄，确保只执行一次
	n.inheritOnce.Do(func() {
		n.mutex.Lock()
		defer n.mutex.Unlock()
		ldNumStr := os.Getenv(LISTEN_FD_NUM)
		if ldNumStr == "" {
			return
		}
		ldNum, err := strconv.Atoi(ldNumStr)
		if err != nil {
			retErr = fmt.Errorf("found invalid count value: %s=%s", LISTEN_FD_NUM, ldNumStr)
		}
		// 0-2 分别被os.Stdin os.Stdout os.Stderr占用
		for i := 3; i < 3+ldNum; i++ {
			file := os.NewFile(uintptr(i), "listener")
			listener, err := net.FileListener(file)
			if err != nil {
				file.Close()
				retErr = fmt.Errorf("error inheriting socket fd %d: %s", i, err)
				return
			}
			if err := file.Close(); err != nil {
				retErr = fmt.Errorf("error closing inherit socket fd %d: %s", i, err)
				return
			}
			n.inherited = append(n.inherited, listener)
		}
	})
	return retErr
}

func (n *Net) activeListener() ([]net.Listener, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	listeners := make([]net.Listener, len(n.active))
	copy(listeners, n.active)
	return listeners, nil
}

// 接管监听句柄
func (n *Net) Takeover(l net.Listener) (*net.Listener, error) {
	if err := n.inherit(); err != nil {
		return nil, err
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	//如果与从旧进程继承的监听句柄相同，则沿用继承来的监听句柄
	for i, listener := range n.inherited {
		if listener == nil {
			continue
		}
		if compare(l.Addr(), listener.Addr()) {
			n.inherited[i] = nil
			n.active = append(n.active, listener)
			return &listener, nil
		}
	}

	n.active = append(n.active, l)
	return &l, nil
}

func (n *Net) StartProcess() (int, error) {
	listeners, err := n.activeListener()
	if err != nil {
		return 0, err
	}

	files := make([]*os.File, len(listeners))
	for i, listener := range listeners {
		if files[i], err = listener.(filer).File(); err != nil {
			return 0, err
		}
		defer files[i].Close()
	}

	cmd, err := which(os.Args[0])
	if err != nil {
		return 0, err
	}

	// 新进程继承旧进程环境变量
	var env []string
	for _, kv := range os.Environ() {
		// 排除监听句柄数
		if !strings.HasPrefix(kv, fmt.Sprintf("%s=", LISTEN_FD_NUM)) {
			env = append(env, kv)
		}
	}
	env = append(env, fmt.Sprintf("%s=%d", LISTEN_FD_NUM, len(listeners)))

	// 继承文件句柄
	allFiles := append([]*os.File{os.Stdin, os.Stdout, os.Stderr}, files...)
	originalWD, _ := os.Getwd()
	process, err := os.StartProcess(cmd, os.Args, &os.ProcAttr{
		Files: allFiles,
		Env:   env,
		Dir:   originalWD,
	})
	if err != nil {
		return 0, err
	}
	return process.Pid, nil
}
