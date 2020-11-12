# mlflow-migrate

## 背景

适用于 mlflow Tracking Server 元数据 迁移(从file store backend迁移到 database-backed store)
## 前提
 mlflow == 1.4.0 

本文参考了[migrate_data.py.py](https://gist.github.com/weldpua2008/7f0c4644d247bd0fc7ba9a83c2d337d5)，如果使用原来的版本，在我公司的生产环境下使用不了，所以进行了改写 
## 使用
  

```
python migrate_data.py \
    --wipe-db \
    --mlruns-dir /path/to/mlruns > /tmp/migration_inserts_full.sql
```
解释说明

名词|解释
---|---
wipe-db|清空数据中的表
mlruns-dir |元数据的存储目录,对应的是mlflow启动服务的--backend-store-uri项

注意：如果/path/to/mlruns 研发人员接触不到 可以让对应的运维人员从生产环境copy下来
