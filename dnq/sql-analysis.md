# Updated SQL Query with Comprehensive Regex

Here's the updated SQL query incorporating the new comprehensive regex pattern:

```sql
WITH reason_patterns AS (
  SELECT 1 as priority, 'No UC Diagnosis' as category, '(?:does\s+not\s+have\s+(?:a\s+)?)?uc\s+diagnosis|never\s+diagnosed\s+with\s+uc|no\s+active\s+(?:uc\s+)?disease|does\s+not\s+have\s+uc|no\s+uc|not\s+diagnosed\s+with\s+uc' as pattern
  UNION ALL SELECT 2, 'Comorbidity', 'comorbidities?'
  UNION ALL SELECT 3, 'No longer Interested', '(?:no\s+longer|not)\s+interested|hung\s+up|not\s+ready'
  UNION ALL SELECT 4, 'Stelara', 'stelara'
  UNION ALL SELECT 5, 'Not in Flare', 'not\s+(?:in|.*\s+)flare|not\s+symptoms'
  UNION ALL SELECT 6, 'No Show / Unable to Reach', 'no\s+show|unable\s+to\s+(?:reach|contact)'
  UNION ALL SELECT 7, 'Exclusion', 'exclusion\s+(?:\#|criteria)\s*\d*'
  UNION ALL SELECT 8, 'Not on Required Medication', 'not\s+(?:on|taken)\s+(?:required\s+)?medic(?:ation|ine)|non\s*compliant\s+with\s+medic(?:ation|ine)'
  UNION ALL SELECT 9, 'Inadequate Documentation', 'inadequate\s+documentation|unable\s+to\s+provide\s+.*\s+records'
  UNION ALL SELECT 10, 'Transportation Issues', '(?:location|site)\s+(?:is\s+)?(?:too\s+)?far|travel\s+difficulty|another\s+country'
  UNION ALL SELECT 11, 'Participant Not Interested', 'participant\s+(?:not\s+interested|.*ain''t\s+going\s+to\s+participate)'
)
SELECT 
    COALESCE(rp.category, 
             CASE WHEN e.NON_ENRL_RSN_CD IN ('Does Not Meet Eligibility Criteria', 'Other') 
                  THEN 'Other' 
                  ELSE e.NON_ENRL_RSN_CD 
             END) AS NON_ENRL_RSN,
    e.src_sys_stat_updt_desc,
    COUNT(*) as count
FROM 
    PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT_ENRL e
LEFT JOIN LATERAL (
    SELECT category 
    FROM reason_patterns 
    WHERE REGEXP_LIKE(e.src_sys_stat_updt_desc, pattern, 'i')
    ORDER BY priority
    FETCH FIRST 1 ROW ONLY
) rp ON 1=1
WHERE 
    e.STDY_ID IN ('M23-784', 'M23-703')
    AND e.prtcpnt_stat_cd IN ('Contacted - Not Suitable', 'Pre-review Failed', 'Screen Failed')
    AND e.non_enrl_rsn IS NOT NULL
    AND LOWER(e.non_enrl_rsn) != 'test'
    AND LOWER(e.src_sys_stat_updt_desc) != 'test'
GROUP BY 1, 2
ORDER BY COUNT(*) DESC, NON_ENRL_RSN;
```

## Key Changes and Explanations

1. **Reason Patterns CTE**: 
   - We've created a CTE named `reason_patterns` that contains all the regex patterns for each category.
   - Each pattern is assigned a priority, which determines the order in which they'll be checked.

2. **LATERAL Join**:
   - We use a LATERAL join to apply the regex patterns to each row.
   - The join selects the first matching category based on priority.

3. **COALESCE and CASE**:
   - We use COALESCE to select either the matched category from our patterns or the original NON_ENRL_RSN_CD.
   - The CASE statement handles the 'Does Not Meet Eligibility Criteria' and 'Other' categories as before.

4. **Group By and Order By**:
   - We group by both the derived NON_ENRL_RSN and the original src_sys_stat_updt_desc.
   - The results are ordered by count descending and then by NON_ENRL_RSN.

5. **Regex Flags**:
   - We use the 'i' flag in REGEXP_LIKE for case-insensitive matching, eliminating the need for LOWER() functions in the pattern matching.

## Benefits of This Approach

1. **Improved Categorization**: The comprehensive regex patterns should capture more nuanced reasons and categorize them more accurately.

2. **Maintainability**: Adding or modifying categories is as simple as updating the `reason_patterns` CTE.

3. **Performance**: By using a LATERAL join and prioritizing patterns, we ensure efficient matching without needing multiple CASE statements.

4. **Flexibility**: The query still allows for manual categories (from NON_ENRL_RSN_CD) to take precedence when appropriate.

5. **Detailed Output**: By including src_sys_stat_updt_desc in the GROUP BY, we maintain granularity in the results, allowing for easier verification and debugging of the categorization.

This updated query should provide a more accurate and maintainable solution for categorizing non-enrollment reasons based on the descriptive text.
