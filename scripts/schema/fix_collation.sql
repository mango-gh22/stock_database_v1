-- 统一所有表的字符集和排序规则
ALTER TABLE stock_basic_info CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
ALTER TABLE index_info CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
ALTER TABLE stock_index_constituent CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;