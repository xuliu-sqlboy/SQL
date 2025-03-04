-- 设置分隔符为 $$
DELIMITER $$

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS cdm_rpa_ViewToTable $$

-- 创建存储过程
CREATE
    DEFINER = root@`%`
    PROCEDURE cdm_rpa_ViewToTable()
BEGIN
    DECLARE view_name VARCHAR(255);
    DECLARE target_table_name VARCHAR(255);
    DECLARE view_cols, table_cols TEXT;
    DECLARE sql_stmt TEXT;

    -- 定义视图和目标表的名称
    SET view_name = 'v_cdm_table_update_status';
    SET target_table_name = 'cdm_table_update_status';

    -- 获取视图的字段列表
    SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) INTO view_cols
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = view_name AND TABLE_SCHEMA = DATABASE();

    -- 获取目标表的字段列表
    SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) INTO table_cols
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = target_table_name AND TABLE_SCHEMA = DATABASE();

    -- 比较字段列表
    IF view_cols = table_cols THEN
        -- 如果字段结构一致，则清空目标表并插入数据
        SET sql_stmt = CONCAT('TRUNCATE TABLE ', target_table_name);
        PREPARE stmt FROM sql_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET sql_stmt = CONCAT('INSERT INTO ', target_table_name, ' SELECT * FROM ', view_name);
        PREPARE stmt FROM sql_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    ELSE
        -- 如果字段结构不一致，则删除目标表并根据视图创建新表
        SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_table_name);
        PREPARE stmt FROM sql_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET sql_stmt = CONCAT('CREATE TABLE ', target_table_name, ' AS SELECT * FROM ', view_name);
        PREPARE stmt FROM sql_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END $$

-- 恢复分隔符为 ;
DELIMITER ;