select survey_id, count(*) listings, sum(reviews) reviews, sum(reviews * price * 5) est_income, avg(rating_end) as rating
from
(   select survey_id, room_id, host_id,
          case when reviews_end - reviews_start >= 0
          then reviews_end - reviews_start
          else NULL
          end as reviews,
          price,
          rating_end - rating_start as rating_diff,
          rating_end
   from
   ( select r.survey_id, room_id, host_id, survey_date,
     first_value(reviews) over w as reviews_start, 
     last_value(reviews) over w as reviews_end,
     avg(price) over w as price,
     first_value(overall_satisfaction) over w as rating_start,
     last_value(overall_satisfaction) over w as rating_end
     from listing_dc r join survey s
     on r.survey_id = s.survey_id
     -- where room_type = 'Private room'
     -- and s.survey_date > '2016-01-01'
     window w as
	          (partition by room_id
         	   order by r.survey_id
	           rows between 1 preceding and current row)
     order by room_id 
   ) as t
) as t2
group by survey_id
order by survey_id