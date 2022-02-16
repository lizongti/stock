(
    select code, score1 + score2 + score3 + score4 + score5 + score6 + score7 + score8
    select days.code,
    case when days.close > ma.ma5 then 1 else 0 end as score1,
    case when days.close > ma.ma10 then 1 else 0 end as score2,
    case when days.close > ma.ma20 then 1 else 0 end as score3,
    case when days.close > ma.ma30 then 1 else 0 end as score5,
    case when days.close > ma.ma40 then 1 else 0 end as score5,
    case when days.close > ma.ma60 then 1 else 0 end as score6,
    case when days.close > ma.ma120 then 1 else 0 end as score7,
    case when days.close > ma.ma240 then 1 else 0 end as score8
    from
    (select code, ma5 from postgresql.stock.moving_average where date in (select date from dates where n=1)) ma,
    (select code, close from postgresql.stock.days where date in (select date from dates where n=1)) days
    where ma.code=days.code and days.close > ma.ma5
) rule7
