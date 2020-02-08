DROP TABLE IF EXISTS TMP_MONTH_NODUPS;

CREATE TABLE TMP_MONTH_NODUPS 
AS select MINUTS_INFO.*
,  row_number()  OVER (PARTITION BY MatchID, Status
                    ORDER BY ((HostGoalsFT+GuestGoalsFT)*1000)  
+HostCornersFT
+GuestCornersFT
+ HostoffTargetFT 
+ GuestoffTargetFT 
+ GuestonTargetFT
+HostonTargetFT
+GuestDAttacksFT
+HostDAttacksFT
+GuestAttacksFT
+HostAttacksFT  DESC) AS ROWNUMB
from MINUTS_INFO
where cast( replace(  cast( date_trunc('month', DATE('{{ ds }}')) as varchar(7))  , '-', '') as varchar(6)) = 
cast(day / 100 as varchar(6)); --RETREIVING MONTH FROM DAY.