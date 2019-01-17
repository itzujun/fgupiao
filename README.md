# fgupiao
百度股票分布式爬虫

## 开始

### 启动 redis

```
 .\redis-server.exe .\redis.windows.conf
```
![image](bmp\redis.png)

### 启动 worker

```
celery -A wtask worker --loglevel=info
```

![image](bmp\worker.jpg)

### 运行客户端
```
python fenbus.py  #运行客户端
```

### 运行效果图
![image](bmp\online.jpg)

### 运行结果
![image](bmp\result.jpg)














