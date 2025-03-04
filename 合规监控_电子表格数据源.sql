create or replace view V_地推_合规监控_电子表格_数据源 as
WITH
    OrderDetails AS (
        SELECT
            dt.订单类型 AS OD_订单类型,
            DT.烽火台订单号 AS od_订单号,
            DT.TD人员 OD_TD人员,
            RPA.客户id OD_客户id,
            FH.日期_公式生成 OD_日期_公式生成,
            FH.城市 OD_城市,
            fh.商家ID od_商家ID,
            fh.商家名称 od_商家名称,
            fh.下单时间 od_下单时间,
            --  STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s') AS formatted_datetime,
            TIMESTAMPDIFF(MINUTE,
                          STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'),
                          LEAD(fh.下单时间) OVER (PARTITION BY FH.商家ID, fh.日期_公式生成, fh.城市 ORDER BY  FH.商家ID DESC, FH.城市 DESC, FH.日期_公式生成,FH.下单时间 ASC)) AS od_下单时间间隔,
            COUNT(RPA.客户id) OVER (PARTITION BY RPA.客户id,fh.城市,fh.日期_公式生成 ) AS od_客户ID累计出现次数,
            COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id, FH.日期_公式生成) AS od_当日下单次数,
            CASE
                WHEN ABS(FH.日期_公式生成 - LEAD(fh.日期_公式生成) OVER (PARTITION BY RPA.客户id ORDER BY  FH.日期_公式生成 asc ,客户id desc,下单时间 desc)) < 2 THEN '否'
                WHEN ABS(FH.日期_公式生成 - LEAD(fh.日期_公式生成) OVER (PARTITION BY RPA.客户id ORDER BY  FH.日期_公式生成 asc ,客户id desc,下单时间 desc)) IS NULL THEN '是'
                ELSE '是'
                END  od_同一客户ID下单间隔天数是否达标,
            COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id ORDER BY FH.日期_公式生成 ASC RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) AS od_近三天出现次数,
            COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id ORDER BY FH.日期_公式生成 ASC RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) AS od_近七天出现次数,
            CASE WHEN fh.导航距离 >= 1000 THEN '合规' ELSE fh.导航距离 END AS od_收餐地址距离商家导航距离,
            CASE WHEN COUNT(DT.城市) OVER (PARTITION BY fh.城市, fh.商家ID, fh.日期_公式生成) > CASE WHEN (sj.订单量（GMV） * 0.5) =0 THEN 1 ELSE (sj.订单量（GMV） * 0.5) END THEN '否' ELSE '是' END AS od_单商家当日TD量是否合规,
            CASE
                WHEN (CASE
                          WHEN IFNULL(j30.原价交易额, 0) = 0 OR IFNULL(j30.营业天数, 0) = 0 THEN 0
                          ELSE SUM(FH.订单原价) OVER (PARTITION BY fh.商家ID, fh.日期_公式生成) / j30.原价交易额 / j30.营业天数 - 1
                    END) > 1 THEN '否'
                ELSE '是'
                END AS od_单商家当日TD原价GMV是否合规,
            CASE WHEN yc.商家id IS  NULL THEN '否' ELSE '是' END AS od_是否风控商家,
            fh.订单原价 od_订单原价,
            COUNT(FH.订单原价) OVER (PARTITION BY FH.订单原价,fh.商家ID, fh.日期_公式生成, FH.城市) AS od_同一商家TD原价重复次数,
            fh.订单金额 od_订单金额,
            CASE
                WHEN IFNULL(fh.订单原价, 0) = 0 THEN 0
                ELSE (fh.订单原价 - fh.订单金额) / fh.订单原价
                END AS od_折扣力度,
            FH.骑手 od_骑手姓名,
            FH.送达时间 od_送达时间,
            CASE
                WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(FH.送达时间, '%Y-%m-%d %H:%i:%s')) >= 20 THEN '合规'
                ELSE TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(FH.送达时间, '%Y-%m-%d %H:%i:%s'))
                END AS od_配送时长,
            CASE
                WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(FH.接单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(fh.取货时间, '%Y-%m-%d %H:%i:%s')) >= 8 THEN '合规'
                ELSE TIMESTAMPDIFF(MINUTE, STR_TO_DATE(FH.接单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(fh.取货时间, '%Y-%m-%d %H:%i:%s'))
                END AS od_取餐时长,
            CASE
                WHEN RPA.系统识别骑手到店米 <= 50 THEN '合规'
                ELSE RPA.系统识别骑手到店米
                END AS od_骑手上报到店距离,
            CASE
                WHEN RPA.联系顾客 = '是' THEN '是'
                ELSE '否'
                END AS od_送达前是否联系客户,
            CASE
                WHEN RPA.改派或转单 = '是' THEN '是'
                ELSE '否'
                END AS od_是否有转单或改派情况,
            CASE
                WHEN RPA.骑手已操作送达米 <= 50 THEN '合规'
                ELSE RPA.骑手已操作送达米
                END AS od_骑手操作送达距离,
            CASE
                WHEN RPA.骑手已操作送达是否上报异常 = '是' THEN '是'
                ELSE '否'
                END AS od_是否有上报异常情况,
            CASE
                WHEN (CASE
                          WHEN RPA.骑手已操作送达米 <= 50 THEN '合规'
                          ELSE RPA.骑手已操作送达米
                          END <> '合规' AND CASE
                                                WHEN RPA.骑手已操作送达是否上报异常 = '是' THEN '是'
                                                ELSE '否'
                                                END = '否')
                    OR
                     CASE
                         WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(FH.送达时间, '%Y-%m-%d %H:%i:%s')) >= 20 THEN '合规'
                         ELSE TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(FH.送达时间, '%Y-%m-%d %H:%i:%s'))
                         END <> '合规'
                    OR CASE
                           WHEN RPA.系统识别骑手到店米 <= 50 THEN '合规'
                           ELSE RPA.系统识别骑手到店米
                           END <> '合规'
                    OR CASE
                           WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(FH.接单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(fh.取货时间, '%Y-%m-%d %H:%i:%s')) >= 8 THEN '合规'
                           ELSE TIMESTAMPDIFF(MINUTE, STR_TO_DATE(FH.接单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(fh.取货时间, '%Y-%m-%d %H:%i:%s'))
                           END <> '合规'
                    OR CASE
                           WHEN RPA.联系顾客 = '是' THEN '是'
                           ELSE '否'
                           END = '否'
                    OR CASE
                           WHEN RPA.改派或转单 = '是' THEN '是'
                           ELSE '否'
                           END = '是'
                    THEN '否'
                ELSE '是'
                END AS od_是否配送合规,
            CASE
                WHEN  (COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id ORDER BY FH.日期_公式生成 ASC RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) >2)
                    OR  (COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id ORDER BY FH.日期_公式生成 ASC RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) >4)
                    OR  (fh.导航距离 < 1000)
                    OR ((CASE
                             WHEN ABS(FH.日期_公式生成 - LEAD(fh.日期_公式生成) OVER (PARTITION BY RPA.客户id ORDER BY  FH.日期_公式生成 asc ,客户id desc,下单时间 desc)) < 2 THEN '否'
                             WHEN ABS(FH.日期_公式生成 - LEAD(fh.日期_公式生成) OVER (PARTITION BY RPA.客户id ORDER BY  FH.日期_公式生成 asc ,客户id desc,下单时间 desc)) IS NULL THEN '是'
                             ELSE '是'
                        END) = '否')
                    THEN '否'
                ELSE '是'
                END AS od_是否下单合规,
            -- 是否商家端合规
            CASE
                WHEN (CASE WHEN COUNT(DT.城市) OVER (PARTITION BY fh.城市, fh.商家ID, fh.日期_公式生成) > CASE WHEN (sj.订单量（GMV） * 0.5) =0 THEN 1 ELSE (sj.订单量（GMV） * 0.5) END THEN '否' ELSE '是' END) = '否'
                    OR
                     (CASE
                          WHEN (CASE
                                    WHEN IFNULL(j30.原价交易额, 0) = 0 OR IFNULL(j30.营业天数, 0) = 0 THEN 0
                                    ELSE SUM(FH.订单原价) OVER (PARTITION BY fh.商家ID, fh.日期_公式生成) / j30.原价交易额 / j30.营业天数 - 1
                              END) > 1 THEN '否'
                          ELSE '是'
                         END) = '否'
                    OR (CASE WHEN yc.商家id IS  NULL THEN '否' ELSE '是' END) = '是'
                    OR (fh.订单原价 > 180) --
                    OR COUNT(FH.订单原价) OVER (PARTITION BY FH.订单原价,fh.商家ID, fh.日期_公式生成, FH.城市) > 2
                    OR (TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), LEAD(fh.下单时间) OVER (PARTITION BY FH.商家ID, fh.日期_公式生成, fh.城市 ORDER BY FH.下单时间 ASC, FH.商家ID DESC, FH.城市 DESC, FH.日期_公式生成))) < 20 --
                    THEN '否'
                ELSE '是'
                END AS od_是否商家端合规,
            --   COUNT(FH.订单原价) OVER (PARTITION BY FH.订单原价,fh.商家ID, fh.日期_公式生成, FH.城市)  as aaaaaa,
            --   是否合规订单
            case when (CASE
                           WHEN (CASE
                                     WHEN RPA.骑手已操作送达米 <= 50 THEN '合规'
                                     ELSE RPA.骑手已操作送达米
                                     END <> '合规' AND CASE
                                                           WHEN RPA.骑手已操作送达是否上报异常 = '是' THEN '是'
                                                           ELSE '否'
                                                           END = '否')
                               OR
                                CASE
                                    WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(FH.送达时间, '%Y-%m-%d %H:%i:%s')) >= 20 THEN '合规'
                                    ELSE TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(FH.送达时间, '%Y-%m-%d %H:%i:%s'))
                                    END <> '合规'
                               OR CASE
                                      WHEN RPA.系统识别骑手到店米 <= 50 THEN '合规'
                                      ELSE RPA.系统识别骑手到店米
                                      END <> '合规'
                               OR CASE
                                      WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(FH.接单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(fh.取货时间, '%Y-%m-%d %H:%i:%s')) >= 8 THEN '合规'
                                      ELSE TIMESTAMPDIFF(MINUTE, STR_TO_DATE(FH.接单时间, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(fh.取货时间, '%Y-%m-%d %H:%i:%s'))
                                      END <> '合规'
                               OR CASE
                                      WHEN RPA.联系顾客 = '是' THEN '是'
                                      ELSE '否'
                                      END = '否'
                               OR CASE
                                      WHEN RPA.改派或转单 = '是' THEN '是'
                                      ELSE '否'
                                      END = '是'
                               THEN '否'
                           ELSE '是'
                END) = '否'
                OR (CASE
                        WHEN  (COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id ORDER BY FH.日期_公式生成 ASC RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) >2)
                            OR  (COUNT(RPA.客户id) OVER (PARTITION BY fh.城市, RPA.客户id ORDER BY FH.日期_公式生成 ASC RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) >4)
                            OR  (fh.导航距离 < 1000)
                            OR ((CASE
                                     WHEN ABS(FH.日期_公式生成 - LEAD(fh.日期_公式生成) OVER (PARTITION BY RPA.客户id ORDER BY  FH.日期_公式生成 asc ,客户id desc,下单时间 desc)) < 2 THEN '否'
                                     WHEN ABS(FH.日期_公式生成 - LEAD(fh.日期_公式生成) OVER (PARTITION BY RPA.客户id ORDER BY  FH.日期_公式生成 asc ,客户id desc,下单时间 desc)) IS NULL THEN '是'
                                     ELSE '是'
                                END) = '否')
                            THEN '否'
                        ELSE '是'
                    END) = '否'
                OR ( CASE
                         WHEN (CASE WHEN COUNT(DT.城市) OVER (PARTITION BY fh.城市, fh.商家ID, fh.日期_公式生成) > CASE WHEN (sj.订单量（GMV） * 0.5) =0 THEN 1 ELSE (sj.订单量（GMV） * 0.5) END THEN '否' ELSE '是' END) = '否'
                             OR
                              (CASE
                                   WHEN (CASE
                                             WHEN IFNULL(j30.原价交易额, 0) = 0 OR IFNULL(j30.营业天数, 0) = 0 THEN 0
                                             ELSE SUM(FH.订单原价) OVER (PARTITION BY fh.商家ID, fh.日期_公式生成) / j30.原价交易额 / j30.营业天数 - 1
                                       END) > 1 THEN '否'
                                   ELSE '是'
                                  END) = '否'
                             OR (CASE WHEN yc.商家id IS  NULL THEN '否' ELSE '是' END) = '是'
                             OR (fh.订单原价 > 180) --
                             OR COUNT(FH.订单原价) OVER (PARTITION BY FH.订单原价,fh.商家ID, fh.日期_公式生成, FH.城市) > 2
                             OR (TIMESTAMPDIFF(MINUTE, STR_TO_DATE(fh.下单时间, '%Y-%m-%d %H:%i:%s'), LEAD(fh.下单时间) OVER (PARTITION BY FH.商家ID, fh.日期_公式生成, fh.城市 ORDER BY FH.下单时间 ASC, FH.商家ID DESC, FH.城市 DESC, FH.日期_公式生成))) < 20 --
                             THEN '否'
                         ELSE '是'
                    END) = '否'
                     then '否' else '是' end od_是否合规订单

                ,
            1.0 / COUNT(客户id) OVER (PARTITION BY 客户id, fh.城市, fh.日期_公式生成) AS od_当日不重复客户ID数,
            1.0 / COUNT(客户id) OVER (PARTITION BY 客户id, fh.日期_公式生成) AS od_当日公司不重复客户ID数,
            sj.一级品类 od_一级品类,
            sj.二级品类 od_二级品类,
            CASE WHEN sj.一级品类 IN ('美食', '甜点', '饮品') THEN '是' ELSE '否' END AS od_是否为餐饮,
            CASE WHEN FH.城市 IS NOT NULL THEN '是' ELSE '否' END AS od_是否TD订单
        FROM mt_cdm.合规监控_填报_地推订单 DT
                 LEFT JOIN mt_cdm.合规监控_填报_烽火台已送达订单 FH ON dt.烽火台订单号 = fh.订单号
                 LEFT JOIN mt_cdm.合规监控_rpa执行结果表 RPA ON RPA.订单号 = DT.烽火台订单号
                 LEFT JOIN mt_cdm.商家日基础数据 sj ON fh.商家ID = sj.商家ID AND STR_TO_DATE(REPLACE(sj.日, '-', ''), '%Y%m%d') = SUBDATE(STR_TO_DATE(REPLACE(fh.日期_公式生成, '-', ''), '%Y%m%d'), INTERVAL 1 DAY)
                 LEFT JOIN (
            SELECT
                b.外卖组织结构,
                b.一级商家配送类型,
                b.一级品类,
                b.二级品类,
                b.三级品类,
                b.商家类型,
                b.代理商家分级标签 AS '分级',
                a.营业天数,
                a.商家ID,
                b.商家名称,
                b.合作BD AS 'BD',
                a.`过去30天`,
                a.`原价交易额`,
                a.`实付交易额`,
                a.`订单量`
            FROM (
                     SELECT
                         CONCAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY), '至', DATE_SUB(CURDATE(), INTERVAL 1 DAY)) AS '过去30天',
                         `商家ID`,
                         COUNT(1) AS '营业天数',
                         ROUND(SUM(原价交易额（GMV）), 2) AS 原价交易额,
                         ROUND(SUM(实付交易额（GMV）), 2) AS 实付交易额,
                         SUM(订单量（GMV）) AS 订单量
                     FROM `商家日基础数据`
                     WHERE STR_TO_DATE(`日`, '%Y%m%d') >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                     GROUP BY `商家ID`
                 ) a
                     LEFT JOIN `商家组织架构` b ON a.`商家ID` = b.`商家ID`
        ) j30 ON fh.商家ID = j30.商家ID
                 LEFT JOIN mt_cdm.异常商家明细_月 yc ON fh.商家ID = yc.商家ID AND DATE_FORMAT(DATE_SUB(fh.日期_公式生成, INTERVAL 1 MONTH), '%Y%m') = DATE_FORMAT(STR_TO_DATE(CONCAT(yc.月, '01'), '%Y%m%d'), '%Y%m')


        WHERE 1 = 1
          and dt.订单类型 = '地推'
          AND dt.烽火台订单号 NOT IN (SELECT 合规监控_填报_取消的订单.订单号 FROM mt_cdm.合规监控_填报_取消的订单)
          AND dt.城市_公式生成 <> '暂无匹配项'
          AND dt.status = 1
    ) ,
    A AS (
        SELECT
            OD_订单类型 AS 订单类型,
            od_订单号 AS 订单号,
            od_TD人员 AS TD人员 ,
            od_客户id AS 客户id ,
            od_日期_公式生成 AS 日期_公式生成 ,
            od_城市 AS 城市 ,
            od_商家ID AS 商家ID ,
            od_商家名称 AS  商家名称 ,
            od_下单时间 AS  下单时间 ,
            od_下单时间间隔 AS 下单时间间隔 ,
            od_客户ID累计出现次数 AS 客户ID累计出现次数 ,
            od_当日下单次数 AS 当日下单次数 ,
            od_同一客户ID下单间隔天数是否达标 AS 同一客户ID下单间隔天数是否达标 ,
            od_近三天出现次数 AS 近三天出现次数 ,
            od_近七天出现次数 AS 近七天出现次数 ,
            od_收餐地址距离商家导航距离 AS 收餐地址距离商家导航距离 ,
            od_单商家当日TD量是否合规 AS 单商家当日TD量是否合规 ,
            od_单商家当日TD原价GMV是否合规 AS 单商家当日TD原价GMV是否合规 ,
            od_是否风控商家 AS 是否风控商家 ,
            od_订单原价 AS 订单原价 ,
            od_同一商家TD原价重复次数 AS 同一商家TD原价重复次数 ,
            od_订单金额 AS 订单金额 ,
            od_折扣力度 AS 折扣力度 ,
            od_骑手姓名 AS 骑手姓名 ,
            od_送达时间 AS 送达时间 ,
            od_配送时长 AS 配送时长 ,
            od_取餐时长 AS 取餐时长 ,
            od_骑手上报到店距离 AS  骑手上报到店距离 ,
            od_送达前是否联系客户 AS 送达前是否联系客户 ,
            od_是否有转单或改派情况 AS 是否有转单或改派情况 ,
            od_骑手操作送达距离 AS 骑手操作送达距离 ,
            od_是否有上报异常情况 AS 是否有上报异常情况 ,
            od_是否配送合规 AS 是否配送合规,
            od_是否下单合规 AS 是否下单合规,
            od_是否商家端合规 AS 是否商家端合规,
            od_是否合规订单 AS 是否合规订单,
            od_当日不重复客户ID数 AS 当日不重复客户ID数 ,
            od_当日公司不重复客户ID数 AS 当日公司不重复客户ID数 ,
            od_一级品类 AS 一级品类 ,
            od_二级品类 AS 二级品类 ,
            od_是否为餐饮 AS 是否为餐饮 ,
            od_是否TD订单 AS 是否TD订单
        FROM OrderDetails

        where 1 = 1
        order by od_商家ID desc,od_城市 desc , od_日期_公式生成 asc , od_下单时间 asc)
select
    o.BP,
    o.三级架构,
    a.日期_公式生成,
    count(*) AS 当日订单数,
    round (sum(a.订单原价),2) as  原价交易额,
    round (sum(a.订单金额),2) as  实付交易额,
    round(case when ifnull(sum(a.订单原价),0) = 0 OR count(*) = 0  then 0 else sum(a.订单原价) / count(*) end,2) AS 原价单均价,
    round(case when ifnull(sum(a.订单金额),0) = 0 OR count(*) = 0  then 0 else sum(a.订单金额) / count(*) end,2) AS 实付单均价,
    round(sum(a.当日不重复客户ID数),2) as 当日不重复客户ID数,
    round(case when ifnull(sum(ifnull(a.当日不重复客户ID数,0)),0) = 0 then 0 else  count(*) / sum(ifnull(a.当日不重复客户ID数,0)) end,2) as 下单频次,
    sum(case  when a.是否合规订单 = '否' then 1 else  0 end)  AS 不合规订单数,
    CASE WHEN count(*) = 0 THEN 0 ELSE  sum(case  when a.是否合规订单 = '否' then 1 else  0 end)/count(*) END AS 不合规订单量占比,
    SUM(case when a.是否下单合规 = '否' THEN 1 ELSE 0 END) AS 下单不合规订单数,
    CASE WHEN  SUM(case when a.是否下单合规 = '否' THEN 1 ELSE 0 END) = 0 THEN 0 ELSE SUM(case when a.是否下单合规 = '否' THEN 1 ELSE 0 END)/  sum(case  when a.是否合规订单 = '否' then 1 else  0 end) END AS 下单不合规订单量占比,
    sum(case when  近三天出现次数 > 2 then 1 else 0 end) AS 同一客户ID近3天下单次数超2次,
    sum(case when a.同一客户ID下单间隔天数是否达标 = '是' THEN 1 ELSE  0 end) AS 同一客户ID下单间隔天数是否达标,
    sum(case when a.近七天出现次数 > 4 then 1 else 0 end) as 同一客户ID近7天下单次数超4次,
    sum(case when a.收餐地址距离商家导航距离 < 1000 then 1 else 0 end ) as 同一客户ID收餐地址距离商家导航距离小于1000米,
    sum(case when a.是否商家端合规 = '否' THEN 1 ELSE 0 END) as 商家端不合规数量,
    case when sum(case  when a.是否合规订单 = '否' then 1 else  0 end) = 0 then 0 else
        sum(case when a.是否商家端合规 = '否' THEN 1 ELSE 0 END)/sum(case  when a.是否合规订单 = '否' then 1 else  0 end) end AS 商家端不合规数量占比,
    sum(case when a.单商家当日TD量是否合规 = '否' THEN 1 ELSE 0 END) as 单商家当日订单量不合规,
    sum(case when a.单商家当日TD原价GMV是否合规 = '否' THEN 1 ELSE 0 END) as 单商家当日订单原价GMV不合规,
    sum(case when a.是否风控商家 = '是' THEN 1 ELSE 0 END) as 上月风控商家进行订单,
    sum(case when a.订单原价 > 180 then 1 else 0 end) AS 订单金额超180,
    SUM(CASE WHEN A.同一商家TD原价重复次数 > 2 THEN 1 ELSE 0 END) AS 同一商家订单原价重复次数超2次,
    SUM(CASE WHEN a.折扣力度 > 0.2 THEN 1 ELSE 0 END ) as 折扣力度超2折,
    sum(case when a.下单时间间隔 < 20 THEN 1 ELSE 0 END ) AS 下单时间间隔小于20分钟,
    SUM(CASE WHEN a.是否配送合规 = '否' THEN 1 ELSE 0 END) as 配送不合规数量,
    sum(case when a.是否配送合规 = '否' THEN 1 ELSE 0 END)/sum(case  when a.是否合规订单 = '否' then 1 else  0 end) AS 配送不合规订单量占比,
    sum(case when a.配送时长 < 20 THEN 1 ELSE 0 END) AS 配送时长小于20分钟,
    sum(case when a.取餐时长 < 8 THEN 1 ELSE 0 END) as 取餐时长小于8分钟,
    sum(case when a.骑手上报到店距离 > 50 THEN 1 ELSE 0 END) as 骑手上报到店距离大于50米,
    sum(case when a.送达前是否联系客户 = '否' THEN 1 ELSE 0 END) as 未联系客户,
    sum(case when a.是否有转单或改派情况 = '是' THEN 1 ELSE 0 END) as 有转单或改派情况,
    sum(case when a.骑手操作送达距离 > 50 and a.是否有上报异常情况 = '否' THEN 1 ELSE 0 END) as 骑手操作送达距离大于50米未上报异常情况
from a
         left join mt_cdm.组织架构 o
                   on o.烽火台城市名称 = a.城市 or o.三级架构 = a.城市
group by  o.BP,
          o.三级架构,
          a.日期_公式生成



