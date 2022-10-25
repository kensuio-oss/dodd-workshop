import os
import shutil



def create_properties_files(trainee, ingestion_url ="", ingestion_token ="", api_token="", api_url ="", offline= False):
    
    if not offline and (ingestion_url == "" or ingestion_token == ""):
        raise Exception("Missing ingestion_url or ingestion_token")
        
    
    if (ingestion_url == "" and ingestion_token != "") or (ingestion_url != "" and ingestion_token == ""):
        raise Exception("Missing ingestion or api configuration")
    
        

    prop = """
    
    user_name=%s
    
    kensu_ingestion_url=%s
    kensu_ingestion_token=%s
    
    kensu.pat=%s
    kensu.sdk.url=%s
    
    environment=workshop
    project_name=%s_dodd
    process_name=%s
    process_run_name=%s
    code_location=binder://workshop-dodd
    code_version=v1

    execution_timestamp=%d

    shorten_data_source_names=true
    data_source_naming_strategy_rules=
    data_source_naming_strategy=File
    logical_data_source_naming_strategy_rules=
    logical_data_source_naming_strategy=File

    input_stats_compute_quantiles=false
    input_stats_compute_quantiles=false
    input_stats_cache_by_path=true
    input_stats_coalesce_enabled=true
    input_stats_coalesce_workers=1
    output_stats_compute_quantiles=false
    output_stats_cache_by_path=false
    output_stats_coalesce_enabled=false
    output_stats_coalesce_workers=1

    report_to_file=%s
    offline_file_name=entities.log
    kensu_api_verify_ssl=false
    enable_entity_compaction=true
    compute_stats=true
    compute_input_stats=true
    input_stats_fallback_to_all_columns_if_not_present_in_lineage=true
    input_stats_for_only_used_columns=false
    shutdown_timeout_sec=600

    collector_log_level=INFO
    collector_log_file_path=debug.log
    collector_log_include_spark_logs=false

    spark_environment_provider=io.kensu.sparkcollector.system.DefaultSparkEnvironmentProvider
    """


    apps = ["Data Preparation", "KPI"]
    weeks = [1635112800000,1635717600000,1636236000000,1636934400000]
    i = 0
    for app in apps:
        for week in weeks:
            p = prop % (trainee, ingestion_url, ingestion_token, api_token, api_url, trainee, app, app + trainee, week, str(offline).lower())
            with open("conf/application%d-week%d.properties" % (apps.index(app)+1, weeks.index(week)+1), "w") as f:
                f.write(p)




