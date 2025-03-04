MySQL正式库：
1.194.161.226:19833
用户名：root
密码：MThqy@156768

--时间修改
SELECT DATE_FORMAT(NOW(), '%Y%m%d') AS formatted_date;
SELECT DATE_SUB(NOW(), INTERVAL 5 MINUTE);--获取5分钟之前的日期




-- 1、批量删除临时表
SET @tables = (
    SELECT GROUP_CONCAT(table_name)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'mt_dw' 
      AND table_name LIKE '%temp%'
);

SET @del_stmt = CONCAT('DROP TABLE ', @tables);
PREPARE stmt FROM @del_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- 2、整表修改数据类型
SELECT 
    CONCAT( 'ALTER TABLE 异常骑手 ','MODIFY COLUMN `', COLUMN_NAME, '` VARCHAR(100) COLLATE utf8mb4_0900_ai_ci;') AS modify_stmt 
FROM 
    INFORMATION_SCHEMA.COLUMNS 
WHERE 
    TABLE_SCHEMA = 'mt_cdm' AND TABLE_NAME = '异常骑手';
	

-- 3、批量修改排序方式
SELECT 
    CONCAT(
      'ALTER TABLE `', TABLE_SCHEMA, '`.`', TABLE_NAME, '` ',
      'MODIFY COLUMN `', COLUMN_NAME, '` ',
      COLUMN_TYPE, ' ',
      'CHARACTER SET utf8mb4 ',
      'COLLATE utf8mb4_0900_ai_ci;'
    ) AS alter_statement
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'mt_cdm'
AND DATA_TYPE = 'varchar'
AND COLLATION_NAME = 'utf8mb4_general_ci';


-- 4、拼接存储过程
-- 拼接存储过程
SELECT 
    CONCAT( '`', COLUMN_NAME, '`', ' = VALUES(`', COLUMN_NAME, '`),') AS modify_stmt 
FROM 
    INFORMATION_SCHEMA.COLUMNS 
WHERE 
    TABLE_SCHEMA = 'mt_ods' AND TABLE_NAME = '新签详情';