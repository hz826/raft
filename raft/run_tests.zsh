#!/bin/zsh

# 清空output文件夹
rm -rf output/*
mkdir -p output

# 设置总测试数
TOTAL_TESTS=100

# 记录开始时间
START_TIME=$(date +%s)

# 运行测试并统计成功次数
SUCCESS_COUNT=$(seq 1 $TOTAL_TESTS | xargs -n 1 -P 20 -I {} sh -c "sudo nice -n -10 go test > output/output_{}.log 2>&1 && echo success || (echo test {} failed; echo test {} failed >> output/errors.log)" | grep -c success)

# 记录结束时间
END_TIME=$(date +%s)

# 计算运行时间
ELAPSED_TIME=$((END_TIME - START_TIME))

# 输出成功测试数量、总数和运行时间
cat output/errors.log
echo "成功测试: $SUCCESS_COUNT / $TOTAL_TESTS"
echo "脚本运行时间: $ELAPSED_TIME 秒"
