class DataQualitySqlQueries:

   rows_greater_than_0 = """
        SELECT 
             COUNT(*) 
        FROM public.{};
    """
    
    check_duplicates = """
        SELECT 
            COUNT({}_id) as cnt,
            COUNT(DISTINCT {}_id) as dist_cnt 
        FROM public.{}s;
    """
    
    top_10_users_by_distinct_session = """
                SELECT
                    u.first_name,
                    u.last_name,
                    sp.user_id,
                    COUNT(DISTINCT sp.session_id) as session_count
                FROM public.songplays sp
                 JOIN public.users u
                ON u.user_id = sp.user_id
                GROUP BY 
                    u.first_name,
                    u.last_name,
                    sp.user_id
                ORDER BY session_count DESC
                LIMIT 10;
                """

