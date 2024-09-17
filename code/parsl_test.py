import parsl
from parsl import python_app, bash_app
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor


# # 定义 Parsl 配置
# config = Config(
#     executors=[
#         HighThroughputExecutor(
#             label='htex',
#             workers=1,  # 工作节点数量；根据系统资源进行调整
#             provider={'type': 'local'},  # 本地提供者用于测试
#             # 可以在这里添加其他参数，如批处理系统或资源限制
#         )
#     ],
#     logging_level='DEBUG',  # 启用调试日志以获得更多信息
#     # 可以在这里添加其他全局配置参数（如需要）
# )

parsl.clear()
# 使用上述配置初始化 Parsl
# parsl.load(config)
parsl.load()


@python_app
def hello_python():
    return 'Hello, World_python!'

@bash_app
def hello_bash(stdout='hello_bash.stdout', stderr='hello_bash.stderr'):
    return 'echo "Hello, World_bash!"'

# 执行 Python 应用
result_python = hello_python()
print(f'Python 应用输出:\n{result_python.result()}')

# 执行 Bash 应用
result_bash = hello_bash()
print(f'Bash 应用输出:\n{result_bash.result()}')

# 读取并打印 Bash 应用的标准输出文件内容
with open('hello_bash.stdout', 'r') as f:
    print(f'Bash 应用标准输出:\n{f.read()}')

# 读取并打印 Bash 应用的标准错误文件内容
with open('hello_bash.stderr', 'r') as f:
    print(f'Bash 应用标准错误:\n{f.read()}')
