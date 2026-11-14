"""集中配置：通过环境变量注入，便于 Docker Compose 编排。"""
import os


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# 天平 TCP 连接参数
BALANCE_HOST = _env("BALANCE_HOST", "simulator")
BALANCE_PORT = int(_env("BALANCE_PORT", "9000"))
BALANCE_RECONNECT_SECONDS = float(_env("BALANCE_RECONNECT_SECONDS", "1"))
# 单次 socket 读取的最大字节数（模拟器拆包时单段可能远小于此值）
SOCKET_RECV_BYTES = int(_env("SOCKET_RECV_BYTES", "4096"))

# 模拟器控制 HTTP 接口（网页控制按钮经后端代理转发）
SIMULATOR_CONTROL_URL = _env("SIMULATOR_CONTROL_URL", "http://simulator:9001")

# SQLite 数据库
DB_PATH = _env("DB_PATH", "/data/weighings.db")

# 归档 SQLite 文件目录（默认放在数据库同级的 archives/ 下，随持久卷一起保留）
ARCHIVE_DIR = _env(
    "ARCHIVE_DIR",
    os.path.join(os.path.dirname(DB_PATH) or ".", "archives"),
)

# 判零阈值：|weight| <= ZERO_EPSILON 视为零位
ZERO_EPSILON = float(_env("ZERO_EPSILON", "0.0005"))

# 状态快照里保留的最近事件条数
EVENT_RING_SIZE = int(_env("EVENT_RING_SIZE", "20"))
