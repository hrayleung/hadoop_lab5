# JData分析实验 (Lab05)

## 实验概述

本实验使用Hadoop MapReduce框架对JData电商数据集进行多维度分析，包括：

1. **用户行为分析** - 分析用户在不同时间、品类、品牌上的行为模式
2. **用户画像分析** - 统计用户年龄、性别、等级等分布特征
3. **商品分析** - 分析商品品类、品牌、属性分布
4. **综合分析** - 整合多个分析结果

## 数据集说明

JData数据集包含以下文件：

- `JData_User.csv` - 用户档案数据
- `JData_Product.csv` - 商品数据
- `JData_Action_201602.csv` - 2016年2月用户行为数据
- `JData_Action_201603.csv` - 2016年3月用户行为数据
- `JData_Action_201604.csv` - 2016年4月用户行为数据
- `JData_Comment.csv` - 商品评价数据

## 项目结构

```
lab05/
├── src/main/java/com/hadoop/lab05/
│   ├── model/                    # 数据模型
│   │   ├── UserAction.java       # 用户行为数据模型
│   │   └── UserProfile.java      # 用户档案数据模型
│   ├── mapreduce/                # MapReduce作业
│   │   ├── UserBehaviorMapper.java      # 用户行为分析Mapper
│   │   ├── UserBehaviorReducer.java     # 用户行为分析Reducer
│   │   ├── UserProfileMapper.java       # 用户画像分析Mapper
│   │   ├── UserProfileReducer.java     # 用户画像分析Reducer
│   │   ├── ProductAnalysisMapper.java  # 商品分析Mapper
│   │   └── ProductAnalysisReducer.java # 商品分析Reducer
│   ├── JDataAnalysisDriver.java        # 主驱动程序
│   └── JobChainManager.java            # 作业链管理器
├── pom.xml                       # Maven配置文件
├── run_analysis.sh              # 运行脚本
└── README.md                    # 说明文档
```

## 运行方法

### 1. 编译项目
```bash
cd /home/hadoop/lab05
mvn clean compile package -DskipTests
```

### 2. 运行分析脚本
```bash
./run_analysis.sh
```

### 3. 手动运行
```bash
# 创建HDFS目录
hdfs dfs -mkdir -p /lab05/input
hdfs dfs -mkdir -p /lab05/output

# 上传数据
hdfs dfs -put /home/hadoop/JData/*.csv /lab05/input/

# 运行分析
hadoop jar target/jdata-analysis-1.0-SNAPSHOT.jar \
    com.hadoop.lab05.JDataAnalysisDriver \
    /lab05/input /lab05/output
```

## 分析结果

### 用户行为分析结果
- 按用户统计各种行为类型（浏览、加购物车、下单等）
- 按品类统计用户行为分布
- 按品牌统计用户行为分布
- 按时间统计用户活跃度

### 用户画像分析结果
- 用户年龄分布统计
- 用户性别分布统计
- 用户等级分布统计
- 用户注册时间分布

### 商品分析结果
- 商品品类分布统计
- 商品品牌分布统计
- 商品属性分布统计

## 技术特点

1. **多作业并行处理** - 使用ControlledJob管理作业依赖关系
2. **数据模型封装** - 使用Writable接口实现自定义数据类型
3. **错误处理** - 完善的异常处理和计数器统计
4. **结果格式化** - 结构化的输出格式，便于后续分析

## 扩展功能

- 可以添加更多分析维度（如时间序列分析、用户聚类等）
- 可以集成机器学习算法进行预测分析
- 可以添加可视化组件展示分析结果

## 注意事项

1. 确保Hadoop集群正常运行
2. 确保有足够的HDFS存储空间
3. 大数据集处理可能需要较长时间
4. 建议先用小数据集测试
