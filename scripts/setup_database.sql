-- scripts/setup_database.sql
-- 创建数据库和用户

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS stock_database
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- 2. 创建用户（如果不存在）
CREATE USER IF NOT EXISTS 'stock_user'@'localhost' IDENTIFIED BY 'root1234';

-- 3. 授予权限
GRANT ALL PRIVILEGES ON stock_database.* TO 'stock_user'@'localhost';

-- 4. 刷新权限
FLUSH PRIVILEGES;

-- 5. 显示用户信息
SELECT user, host FROM mysql.user WHERE user = 'stock_user';

-- 6. 显示数据库
SHOW DATABASES LIKE 'stock_database';