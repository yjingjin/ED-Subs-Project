# EDA Results

## 1. Subscriber Distribution by Current Term Status

|term_status_raw|count_subscribers|
|---------------|-----------------|
|active         |18378            |
|canceled       |9410             |
|paused         |276              |

## 2. Overall 30-Day Cancellation Request Summary

|count_subscribers|cnt_cancel_requested_in_30d|cnt_cancel_pct    |
|-----------------|---------------------------|------------------|
|28064            |4518                       |16.098916761687571|

## 3. 30-Day Cancellation Request Summary by Cadence Months

|cadence_months|count_subscribers|cnt_cancel_requested_in_30d|cnt_cancel_pct    |
|--------------|-----------------|---------------------------|------------------|
|1             |13663            |3539                       |25.902071287418575|
|3             |7349             |634                        |8.627024084909511 |
|6             |7052             |345                        |4.892229154849688 |

## 4. Term Status by Cadence Months

|cadence_months|term_status|count_subscribers|count_cancel_requested_in_30d|
|--------------|-----------|-----------------|-----------------------------|
|1             |active     |6304             |312                          |
|1             |canceled   |7256             |3227                         |
|1             |paused     |103              |0                            |
|3             |active     |5608             |183                          |
|3             |canceled   |1683             |451                          |
|3             |paused     |58               |0                            |
|6             |active     |6466             |163                          |
|6             |canceled   |471              |182                          |
|6             |paused     |115              |0                            |

## 5. Plan Change Count by Cadence Months

|cadence_months|plan_change_count|count_subscribers|cnt_cancel_requested_in_30d|cnt_cancel_pct    |
|--------------|-----------------|-----------------|---------------------------|------------------|
|1             |0                |13024            |3448                       |26.474201474201474|
|1             |1                |533              |70                         |13.133208255159474|
|1             |2                |89               |19                         |21.348314606741573|
|1             |3                |11               |2                          |18.181818181818181|
|1             |4                |3                |0                          |0.000000000000000 |
|1             |5                |1                |0                          |0.000000000000000 |
|1             |6                |2                |0                          |0.000000000000000 |
|3             |0                |6819             |593                        |8.696289778559906 |
|3             |1                |425              |35                         |8.235294117647058 |
|3             |2                |90               |5                          |5.555555555555555 |

## 6. Refill Count by Cadence Months

|cadence_months|refill_count|total_subscribers|cnt_cancel_requested|cnt_cancel_pct    |
|--------------|------------|-----------------|--------------------|------------------|
|1             |0           |5277             |2728                |51.696039416335038|
|1             |1           |2637             |374                 |14.182783466059916|
|1             |2           |1554             |13                  |0.836550836550836 |
|1             |3           |911              |2                   |0.219538968166849 |
|1             |4           |714              |0                   |0.000000000000000 |
|1             |5           |1302             |2                   |0.153609831029185 |
|1             |6           |147              |0                   |0.000000000000000 |
|3             |0           |3728             |521                 |13.975321888412017|
|3             |1           |2699             |12                  |0.444609114486846 |
|3             |2           |414              |1                   |0.241545893719806 |
|3             |3           |194              |0                   |0.000000000000000 |
|6             |0           |5541             |264                 |4.764482945316729 |
|6             |1           |1094             |15                  |1.371115173674588 |
|6             |2           |157              |0                   |0.000000000000000 |

## 7. Cancellation Summary by Latest Drug Name

|latest_drug_name  |total_subscribers|cnt_cancel_requested|cnt_cancel_pct    |
|------------------|-----------------|--------------------|------------------|
|sildenafil        |10572            |1980                |18.728717366628830|
|tadalafil (cialis)|17305            |2527                |14.602715978041028|

## 8. Cancellation Summary by Latest Drug Name and Strength

|latest_drug_name  |latest_drug_strength|total_subscribers|cnt_cancel_requested_in_30d|cnt_cancel_pct    |
|------------------|--------------------|-----------------|---------------------------|------------------|
|sildenafil        |25mg                |777              |181                        |23.294723294723294|
|tadalafil (cialis)|10mg                |5524             |1122                       |20.311368573497465|
|sildenafil        |50mg                |6798             |1374                       |20.211827007943512|
|tadalafil (cialis)|2.5mg               |3440             |505                        |14.680232558139534|
|tadalafil (cialis)|20mg                |2409             |352                        |14.611872146118721|
|sildenafil        |100mg               |2997             |425                        |14.180847514180847|
|tadalafil (cialis)|5mg                 |5932             |548                        |9.238031018206338 |

## 9. Cancellation Summary by Visit Count

Visits here are counted from the time subscription is activated till it is cancelled.

|visit_count|total_subscribers|cancelled_subscribers|cancel_rate_pct|
|-----------|-----------------|---------------------|---------------|
|1          |23478            |6881                 |29.31          |
|2          |3479             |525                  |15.09          |
|3          |785              |102                  |12.99          |
|4          |247              |27                   |10.93          |
|5          |56               |9                    |16.07          |
|6          |12               |2                    |16.67          |
|7          |4                |1                    |25.00          |
|8          |1                |0                    |0.00           |
|10         |1                |1                    |100.00         |

## 10. 30-Day Cancellation Request Summary by Visit Count

|visit_count|total_subscribers|cancel_requested_in_30d|cancel_rate_pct|
|-----------|-----------------|-----------------------|---------------|
|1          |23478            |3677                   |15.66          |
|2          |3479             |166                    |4.77           |
|3          |785              |16                     |2.04           |
|4          |247              |0                      |0.00           |
|5          |56               |1                      |1.79           |
|6          |12               |0                      |0.00           |
|7          |4                |0                      |0.00           |
|8          |1                |0                      |0.00           |
|10         |1                |0                      |0.00           |