with org as
(select
    BP,
    三级架构,
    业务部,
    大区域,
    全部,
    `小区域1(公司级)`
from mt_cdm.组织架构),
xt_mon as (
    select
        组织结构,
        月,
        合作商补贴金额 as 月代补率_分子,
        原价交易额 as 月代补率_分母
    from mt_cdm.合作商经营数据
),
xt_ld_mon as (
    SELECT * from (
                      select
                          组织结构,
                          月,
                          合作商补贴金额 as 昨日_月代补率_分子,
                          原价交易额 as 昨日_月代补率_分母,
                          更新时间,
                          row_number() over (partition by 组织结构, 月  order by 更新时间 desc) rk
                      from  mt_ods.合作商经营数据
                      where date_format(substr(更新时间,1,10),'%Y%m%d') = date_format(date_sub(now(), interval 1 day),'%Y%m%d')
                  ) t
    where t.rk = 1

),
xt_cy_mon as (
    -- 系统代补率 餐饮变化趋势
    select 组织结构,
           月,
           合作商补贴金额 as 月代补率_分子,
           原价交易额 as 月代补率_分母
    from mt_cdm.合作商经营数据分品类
    where 业务品类分类 = '餐饮'
) ,
xt_ld_cy_mon as (
    select  * from (
                       select
                           组织结构,
                           月,
                           合作商补贴金额 as 昨日_月代补率_分子,
                           原价交易额 as 昨日_月代补率_分母,
                           更新时间,
                           row_number() over (partition by 组织结构,月 order by 更新时间 desc ) rk
                       from  mt_ods.合作商经营数据分品类
                       where date_format(substr(更新时间,1,10),'%Y%m%d') = date_format(date_sub(now(), interval 1 day),'%Y%m%d')
                         and 业务品类分类 = '餐饮'
                   ) t
    where t.rk = 1
),
xt_fc_mon as (
    select 组织结构,
           月,
           sum(合作商补贴金额) as 月代补率_分子,
           sum(原价交易额) as 月代补率_分母
    from mt_cdm.合作商经营数据分品类
    where 业务品类分类 <> '餐饮'
    group by 组织结构,月
),
    xt_ld_fc_mon as (
        SELECT
            组织结构,
            月,
            sum(月代补率_分子) 月代补率_分子,
            sum(月代补率_分母) 月代补率_分母
        from (
                 select
                     组织结构,
                     月,
                     业务品类分类,
                     更新时间,
                     合作商补贴金额 as 月代补率_分子,
                     原价交易额 as 月代补率_分母,
                     row_number() over (partition by 组织结构,月,业务品类分类 order by 更新时间 desc ) rk
                 from mt_ods.合作商经营数据分品类
                 where date_format(substr(更新时间,1,10),'%Y%m%d') = date_format(date_sub(now(), interval 1 day),'%Y%m%d')
                   and 业务品类分类 <> '餐饮'
             ) T
        where rk = 1
        group by 月,组织结构
    ),
xt_sg_mon as (
    select 组织结构,
           月,
           合作商补贴金额 as 月代补率_分子,
           原价交易额 as 月代补率_分母
    from mt_cdm.合作商经营数据分品类
    where 业务品类分类 = '闪购'
),
    xt_ld_sg_mon as (
        -- 系统代补率_餐饮昨日
        select  * from (
                           select
                               组织结构,
                               月,
                               合作商补贴金额 as 昨日_月代补率_分子,
                               原价交易额 as 昨日_月代补率_分母,
                               更新时间,
                               row_number() over (partition by 组织结构,月 order by 更新时间 desc ) rk
                           from  mt_ods.合作商经营数据分品类
                           where date_format(substr(更新时间,1,10),'%Y%m%d') = date_format(date_sub(now(), interval 1 day),'%Y%m%d')
                             and 业务品类分类 = '闪购'
                       ) t
        where t.rk = 1
    ),
xt_yy_mon as (
    select 组织结构,
           月,
           合作商补贴金额 as 月代补率_分子,
           原价交易额 as 月代补率_分母
    from mt_cdm.合作商经营数据分品类
    where 业务品类分类 = '医药'
),
xt_ld_yy_mon as (
    select  * from (
                       select
                           组织结构,
                           月,
                           合作商补贴金额 as 昨日_月代补率_分子,
                           原价交易额 as 昨日_月代补率_分母,
                           更新时间,
                           row_number() over (partition by 组织结构,月 order by 更新时间 desc ) rk
                       from  mt_ods.合作商经营数据分品类
                       where date_format(substr(更新时间,1,10),'%Y%m%d') = date_format(date_sub(now(), interval 1 day),'%Y%m%d')
                         and 业务品类分类 = '医药'
                   ) t
    where t.rk = 1
)/*,
data as (*/
select org.BP BP ,
       org.三级架构 city,
       org.业务部 ,
       org.大区域 ,
       org.全部,
       org.`小区域1(公司级)` as 小区域 ,
       xt_mon.月 as 月,
       xt_mon.月代补率_分子 as 月代补率_分子,
       xt_mon.月代补率_分母 as 月代补率_分母,
       xt_ld_mon.昨日_月代补率_分子,
       xt_ld_mon.昨日_月代补率_分母,
       xt_cy_mon.月代补率_分子 as 餐饮_月代补率_分子,
       xt_cy_mon.月代补率_分母 as 餐饮_月代补率_分母,
       xt_ld_cy_mon.昨日_月代补率_分子 as 昨日_餐饮_月代补率_分子,
       xt_ld_cy_mon.昨日_月代补率_分母 as 昨日_餐饮_月代补率_分母,
       xt_fc_mon.月代补率_分子 as 非餐饮_月代补率_分子,
       xt_fc_mon.月代补率_分母 as 非餐饮_月代补率_分母,
       xt_ld_fc_mon.月代补率_分子 as 昨日_非餐饮_月代补率_分子,
       xt_ld_fc_mon.月代补率_分母 as 昨日_非餐饮_月代补率_分母,
       xt_sg_mon.月代补率_分子 as 闪购_月代补率_分子,
       xt_sg_mon.月代补率_分母 as 闪购_月代补率_分母,
       xt_ld_sg_mon.昨日_月代补率_分子 as 昨日_闪购_月代补率_分子,
       xt_ld_sg_mon.昨日_月代补率_分母 as 昨日_闪购_月代补率_分母,
       xt_yy_mon.月代补率_分子 as 医药_月代补率_分子,
       xt_yy_mon.月代补率_分母 as 医药_月代补率_分母,
       xt_ld_yy_mon.昨日_月代补率_分子 as 昨日_医药_月代补率_分子,
       xt_ld_yy_mon.昨日_月代补率_分母 as 昨日_医药_月代补率_分母
from org
left join xt_mon
on org.三级架构=xt_mon.组织结构
left join xt_ld_mon
on org.三级架构=xt_ld_mon.组织结构
left join xt_cy_mon
on org.三级架构=xt_cy_mon.组织结构
left join xt_ld_cy_mon
on org.三级架构=xt_ld_cy_mon.组织结构
left join xt_fc_mon
on org.三级架构=xt_fc_mon.组织结构
left join xt_ld_fc_mon
on org.三级架构=xt_ld_fc_mon.组织结构
left join xt_sg_mon
on org.三级架构=xt_sg_mon.组织结构
left join xt_ld_sg_mon
on org.三级架构=xt_ld_sg_mon.组织结构
left join xt_yy_mon
on org.三级架构=xt_yy_mon.组织结构
left join xt_ld_yy_mon
on org.三级架构=xt_ld_yy_mon.组织结构




