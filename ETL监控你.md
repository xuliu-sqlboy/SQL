# cdm-->ads层流转

## 所用表结构

```sql
SELECT * FROM mt_ads.pending_procedures; -- 待执行的存储过程表

建表语句


CREATE TABLE pending_procedures (
    id INT AUTO_INCREMENT PRIMARY KEY,      -- 存储过程ID
    procedure_name VARCHAR(255) NOT NULL,   -- 存储过程名称
    status INT DEFAULT 0,                  -- 状态: 0-待执行, 2-执行失败, 1-执行成功
    error_msg TEXT,                        -- 执行错误信息
    last_attempt_time DATETIME DEFAULT NULL, -- 最后尝试执行时间
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  -- 更新时间
);

```

```sql
select * from mt_ads.executed_procedures; -- 执行成功的存储过程表

建表语句
CREATE TABLE executed_procedures (
    id INT AUTO_INCREMENT PRIMARY KEY,      -- 存储过程ID
    procedure_name VARCHAR(255) NOT NULL,   -- 存储过程名称
    execution_time DATETIME DEFAULT CURRENT_TIMESTAMP,  -- 执行时间
    result TEXT,                            -- 执行结果或其他相关信息
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP -- 创建时间
);


```

- 存储过程

```sql
drop procedure if exists execute_pending_procedures;

DELIMITER $$

CREATE PROCEDURE execute_pending_procedures()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE proc_name VARCHAR(255);
    declare pro_id int;
    DECLARE cur CURSOR FOR
        SELECT procedure_name,id FROM pending_procedures WHERE status in( 0,2);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    -- 循环处理待执行的存储过程
    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO proc_name,pro_id;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- 尝试执行存储过程
        BEGIN
            DECLARE exit handler for SQLEXCEPTION
                BEGIN
                    -- 如果执行失败，更新状态为失败并记录错误信息
                    UPDATE pending_procedures
                    SET status = 2,
                        error_msg = '存储过程执行失败',
                        last_attempt_time = NOW()
                    WHERE procedure_name = proc_name
                    and id = pro_id;

                END;

            -- 执行存储过程
            SET @sql = CONCAT('CALL ', proc_name, '();');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

            -- 如果执行成功，更新状态为已执行并存入执行成功的记录表
            INSERT INTO executed_procedures (procedure_name, execution_time)
            VALUES (proc_name, NOW());

            DELETE FROM pending_procedures WHERE procedure_name = proc_name;
        END;

    END LOOP;

    CLOSE cur;
END$$

DELIMITER ;


```
