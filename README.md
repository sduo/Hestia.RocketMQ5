# Hestia.RocketMQ5

[![](https://github.com/sduo/Hestia.RocketMQ5/actions/workflows/main.yml/badge.svg)](https://github.com/sduo/Hestia.RocketMQ5)
[![](https://img.shields.io/nuget/v/Hestia.RocketMQ5.svg)](https://www.nuget.org/packages/Hestia.RocketMQ5)

# 生产者

## 配置文件结构
```json
{
    "endpoints": "rmq.endpoints.com:8080",
    "ak": "rmq",
    "sk": "************"
}
```

# 消费者

## 配置文件结构

```json
{
    "endpoints": "rmq.endpoints.com:8080",
    "ak": "rmq",
    "sk": "************",
    "group": "gid",
    "topic": "test"
}
```