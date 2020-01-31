from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SQLContext
from awsglue.dynamicframe import DynamicFrame
import pg8000
import boto3
import datetime
import pytz
from pytz import timezone


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
sqlContext = SQLContext(sparkContext=sc)
s3_db_name = 'ca_s3_db'
aws_region = 'us-east-1'
ssm = boto3.client('ssm', region_name = aws_region)
s3 = boto3.client('s3')

port = int(ssm.get_parameter(Name="/ca/db/aurora/qa/port")['Parameter']['Value'])
host = ssm.get_parameter(Name="/ca/db/aurora/qa/host")['Parameter']['Value']
db_user = ssm.get_parameter(Name="/ca/db/aurora/qa/user")['Parameter']['Value']
password = ssm.get_parameter(Name="/ca/db/aurora/qa/password", WithDecryption=True)['Parameter']['Value']
db_name = ssm.get_parameter(Name="/ca/db/aurora/qa/db_name")['Parameter']['Value']

target_table = "geagp_ia.ia_cell_oper_lkp_auto"
target_file = "geagp_ia_ia_cell_oper_lkp_auto"
target_table1 = "geagp_ia.ia_cell_oper_lkp"
target_file1 = "geagp_ia_ia_cell_oper_lkp"
ia_s3_source = "s3://med-av-daas-preprod-ca/S3-base/geagp-ia/"
csf_rt_s3_source = "s3://med-av-daas-preprod-ca/S3-base/geagp-csf-rt/"
glf_s3_source = "s3://med-av-daas-preprod-ca/S3-base/geagp-glf/"
khw_s3_source = "s3://med-av-daas-preprod-ca/S3-base/geagp-khw/"
s3_target = "s3://med-av-daas-preprod-ca/S3-base/geagp-ia/"
s3_target_history = "s3://med-av-daas-preprod-ca/S3-base/geagp-ia-history/"

now = datetime.datetime.now(pytz.timezone('US/Eastern'))
dt = now.strftime("%Y")+"_" + now.strftime("%m")+"_" + now.strftime("%d")+"_" + now.strftime("%H")+"_" + now.strftime("%M")+"_" + now.strftime("%S")


ia_tablename_list = ['geagp_ia.ia_cell_oper_lkp', 'geagp_ia.ca_paradigm_malden_wip', 'geagp_ia.ca_paradigm_malden_throughput', 'geagp_ia.ia_part_cell_lkp'] 
glf_tablename_list = ['mtl_system_items_b', 'wip_entities','wip_operations', 
'mtl_parameters', 'bom_operational_routings','bom_operation_sequences','bom_operation_resources', 'bom_resources']
khw_tablename_list = ['mtl_system_items_b', 'mtl_parameters', 'wip_entities', 'wip_operations']
csf_rt_tablename_list = ['geagp_csf_rt.ca_part_oper_control']

    
for table in ia_tablename_list:
    '''transformation_ctx = table.replace('.', '_') + "_ctx"
    DyF = glueContext.create_dynamic_frame.from_catalog(
        database = s3_db_name, 
        table_name = table.replace('.', '_'), 
        transformation_ctx = transformation_ctx
    )
    df = DyF.toDF()'''
    df = sqlContext.read.parquet(ia_s3_source + table.replace('.', '_'))
    df.createOrReplaceTempView(table.replace('.', '_'))
    
for table in csf_rt_tablename_list:
    '''transformation_ctx = table.replace('.', '_') + "_ctx"
    DyF = glueContext.create_dynamic_frame.from_catalog(
        database = s3_db_name, 
        table_name = table.replace('.', '_'), 
        transformation_ctx = transformation_ctx
    )
    df = DyF.toDF()'''
    df = sqlContext.read.parquet(csf_rt_s3_source + table.replace('.', '_'))
    df.createOrReplaceTempView(table.replace('.', '_'))
    
s3_db_names = {"glf":"snglp_geagp_glf"}

'''for table in glf_tablename_list:
    transformation_ctx = table.replace('.', '_') + "_ctx"
    DyF = glueContext.create_dynamic_frame.from_catalog(
        database = s3_db_name, 
        table_name = table.replace('.', '_'), 
        transformation_ctx = transformation_ctx
    )
    df = DyF.toDF()
    df = sqlContext.read.parquet(glf_s3_source + table.replace('.', '_'))
    df.createOrReplaceTempView(table.replace('.', '_'))'''

for table in glf_tablename_list:
     transformation_ctx = table + "_ctx"
     DyF = glueContext.create_dynamic_frame.from_catalog(
         database = s3_db_names['glf'], 
         table_name = table, 
         transformation_ctx = transformation_ctx
     )
     df = DyF.toDF()
     df.createOrReplaceTempView("geagp_glf_" +table)

wip_op_vw = spark.sql(""" SELECT wip_entity_id,operation_seq_num,organization_id,repetitive_schedule_id,last_update_date,last_updated_by,creation_date,
created_by,last_update_login,request_id,program_application_id,program_id,program_update_date,operation_sequence_id,standard_operation_id,department_id,scheduled_quantity,
quantity_in_queue,quantity_running,quantity_waiting_to_move,quantity_rejected,quantity_scrapped,quantity_completed,first_unit_start_date,first_unit_completion_date,last_unit_start_date,last_unit_completion_date,previous_operation_seq_num,next_operation_seq_num,count_point_type,backflush_flag,minimum_transfer_quantity,date_last_moved,operation_yield,operation_yield_enabled,pre_split_quantity,operation_completed,shutdown_type,disable_date,progress_percentage,employee_id,actual_start_date,actual_completion_date,projected_completion_date
from geagp_glf_wip_operations""")
wip_op_vw.createOrReplaceTempView("geagp_glf_wip_operations_vw")

wip_ents_vw =  spark.sql("""SELECT wip_entity_id,organization_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,request_id,
program_application_id,program_id,program_update_date,wip_entity_name,entity_type,description,primary_item_id,gen_object_id
from geagp_glf_wip_entities""")
wip_ents_vw.createOrReplaceTempView("geagp_glf_wip_entities_vw")

mtl_param_vw = spark.sql("""select organization_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,organization_code,master_organization_id,
primary_cost_method,cost_organization_id,default_material_cost_id,calendar_exception_set_id,calendar_code,general_ledger_update_code,default_atp_rule_id,default_picking_rule_id,default_locator_order_value,default_subinv_order_value,negative_inv_receipt_code,stock_locator_control_code,material_account,material_overhead_account,matl_ovhd_absorption_acct,resource_account,purchase_price_var_account,ap_accrual_account,overhead_account,outside_processing_account,intransit_inv_account,interorg_receivables_account,interorg_price_var_account,interorg_payables_account,cost_of_sales_account,encumbrance_account,project_cost_account,interorg_transfer_cr_account,matl_interorg_transfer_code,interorg_trnsfr_charge_percent,source_organization_id,source_subinventory,source_type,org_max_weight,org_max_weight_uom_code,org_max_volume,org_max_volume_uom_code,serial_number_type,auto_serial_alpha_prefix,start_auto_serial_number,auto_lot_alpha_prefix,lot_number_uniqueness,
lot_number_generation,lot_number_zero_padding,lot_number_length,starting_revision,attribute_category,attribute1,attribute2,attribute3,attribute4,attribute5,attribute6,attribute7,attribute8,attribute9,attribute10,attribute11,attribute12,attribute13,attribute14,
attribute15,default_demand_class,encumbrance_reversal_flag,maintain_fifo_qty_stack_type,invoice_price_var_account,average_cost_var_account,sales_account,expense_account,serial_number_generation,request_id,program_application_id,program_id,program_update_date,global_attribute_category,global_attribute1,global_attribute2,global_attribute3,global_attribute4,global_attribute5,global_attribute6,global_attribute7,global_attribute8,global_attribute9,global_attribute10,global_attribute11,global_attribute12,global_attribute13,
global_attribute14,global_attribute15,global_attribute16,global_attribute17,global_attribute18,global_attribute19,global_attribute20,mat_ovhd_cost_type_id,project_reference_enabled,pm_cost_collection_enabled,project_control_level,avg_rates_cost_type_id,txn_approval_timeout_period,mo_source_required,mo_pick_confirm_required,mo_approval_timeout_action,borrpay_matl_var_account,borrpay_moh_var_account,borrpay_res_var_account,borrpay_osp_var_account,borrpay_ovh_var_account,process_enabled_flag,process_orgn_code,wsm_enabled_flag,default_cost_group_id,lpn_prefix,lpn_suffix,lpn_starting_number,wms_enabled_flag,pregen_putaway_tasks_flag,regeneration_interval,timezone_id,max_picks_batch,default_wms_picking_rule_id,default_put_away_rule_id,default_task_assign_rule_id,default_label_comp_rule_id,default_carton_rule_id,default_cyc_count_header_id,crossdock_flag,
cartonization_flag,cost_cutoff_date,enable_costing_by_category,cost_group_accounting,allocate_serial_flag,default_pick_task_type_id,default_cc_task_type_id,default_putaway_task_type_id,default_repl_task_type_id,eam_enabled_flag,maint_organization_id,prioritize_wip_jobs,default_crossdock_subinventory,skip_task_waiting_minutes,qa_skipping_insp_flag,default_crossdock_locator_id,default_moxfer_task_type_id,default_moissue_task_type_id,default_matl_ovhd_cost_id,distributed_organization_flag,carrier_manifesting_flag,distribution_account_id,direct_shipping_allowed,default_pick_op_plan_id,max_clusters_allowed,consigned_flag,
cartonize_sales_orders,cartonize_manufacturing,defer_logical_transactions,wip_overpick_enabled,ovpk_transfer_orders_enabled,total_lpn_length,ucc_128_suffix_flag,wcs_enabled,auto_del_alloc_flag,rfid_verif_pcnt_threshold,allow_different_status,child_lot_alpha_prefix,child_lot_number_length,child_lot_validation_flag,child_lot_zero_padding_flag,copy_lot_attribute_flag,create_lot_uom_conversion,genealogy_formula_security,parent_child_generation_flag,rules_override_lot_reservation,yard_management_enabled_flag,trading_partner_org_flag,deferred_cogs_account,default_crossdock_criteria_id,enforce_locator_alis_unq_flag,epc_generation_enabled_flag,company_prefix,company_prefix_index,commercial_govt_entity_number,labor_management_enabled_flag,default_status_id,lcm_enabled_flag,lcm_var_account,opsm_enabled_flag,allocate_lot_flag,cat_wt_account
from geagp_glf_mtl_parameters""")
mtl_param_vw.createOrReplaceTempView("geagp_glf_mtl_parameters_vw")

mtl_systm_itm_b_vw = spark.sql("""SELECT inventory_item_id,organization_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,summary_flag,enabled_flag,start_date_active,end_date_active,
CASE WHEN organization_id IN (448, 1048, 1090) THEN description ELSE 'OBFUSCATED' END AS description, buyer_id,accounting_rule_id,invoicing_rule_id,
case when organization_id in (448, 1048, 1090) THEN segment1 ELSE 'OBFUSCATED' END AS segment1,
segment2,segment3,segment4,segment5,segment6,segment7,segment8,segment9,segment10,segment11,segment12,segment13,segment14,segment15,segment16,segment17,segment18,segment19,
segment20,attribute_category,attribute1,attribute2,attribute3,attribute4,attribute5,attribute6,attribute7,attribute8,attribute9,attribute10,attribute11,attribute12,attribute13,attribute14,attribute15,purchasing_item_flag,shippable_item_flag,customer_order_flag,internal_order_flag,service_item_flag,inventory_item_flag,eng_item_flag,inventory_asset_flag,
purchasing_enabled_flag,customer_order_enabled_flag,internal_order_enabled_flag,so_transactions_flag,mtl_transactions_enabled_flag,stock_enabled_flag,bom_enabled_flag,build_in_wip_flag,revision_qty_control_code,item_catalog_group_id,catalog_status_flag,
returnable_flag,default_shipping_org,collateral_flag,taxable_flag,qty_rcv_exception_code,allow_item_desc_update_flag,inspection_required_flag,receipt_required_flag,market_price,hazard_class_id,rfq_required_flag,qty_rcv_tolerance,list_price_per_unit,un_number_id,price_tolerance_percent,asset_category_id,rounding_factor,unit_of_issue,enforce_ship_to_location_code,allow_substitute_receipts_flag,allow_unordered_receipts_flag,allow_express_delivery_flag,days_early_receipt_allowed,days_late_receipt_allowed,receipt_days_exception_code,receiving_routing_id,invoice_close_tolerance,receive_close_tolerance,auto_lot_alpha_prefix,start_auto_lot_number,lot_control_code,shelf_life_code,shelf_life_days,serial_number_control_code,start_auto_serial_number,auto_serial_alpha_prefix,source_type,source_organization_id,source_subinventory,expense_account,encumbrance_account,restrict_subinventories_code,unit_weight,weight_uom_code,volume_uom_code,unit_volume,restrict_locators_code,location_control_code,shrinkage_rate,acceptable_early_days,planning_time_fence_code,demand_time_fence_code,lead_time_lot_size,std_lot_size,cum_manufacturing_lead_time,overrun_percentage,mrp_calculate_atp_flag,acceptable_rate_increase,acceptable_rate_decrease,cumulative_total_lead_time,planning_time_fence_days,demand_time_fence_days,end_assembly_pegging_flag,repetitive_planning_flag,planning_exception_set,bom_item_type,pick_components_flag,replenish_to_order_flag,base_item_id,atp_components_flag,atp_flag,fixed_lead_time,variable_lead_time,wip_supply_locator_id,wip_supply_type,wip_supply_subinventory,primary_uom_code,primary_unit_of_measure,allowed_units_lookup_code,cost_of_sales_account,sales_account,default_include_in_rollup_flag,inventory_item_status_code,inventory_planning_code,planner_code,planning_make_buy_code,fixed_lot_multiplier,rounding_control_type,carrying_cost,postprocessing_lead_time,preprocessing_lead_time,full_lead_time,order_cost,mrp_safety_stock_percent,mrp_safety_stock_code,min_minmax_quantity,max_minmax_quantity,minimum_order_quantity,fixed_order_quantity,fixed_days_supply,maximum_order_quantity,atp_rule_id,picking_rule_id,reservable_type,positive_measurement_error,negative_measurement_error,engineering_ecn_code,engineering_item_id,engineering_date,service_starting_delay,vendor_warranty_flag,serviceable_component_flag,serviceable_product_flag,base_warranty_service_id,payment_terms_id,preventive_maintenance_flag,primary_specialist_id,secondary_specialist_id,serviceable_item_class_id,time_billable_flag,material_billable_flag,expense_billable_flag,prorate_service_flag,coverage_schedule_id,service_duration_period_code,service_duration,warranty_vendor_id,max_warranty_amount,response_time_period_code,response_time_value,new_revision_code,invoiceable_item_flag,tax_code,invoice_enabled_flag,must_use_approved_vendor_flag,
request_id,program_application_id,program_id,program_update_date,
outside_operation_flag,outside_operation_uom_type,safety_stock_bucket_days,auto_reduce_mps,costing_enabled_flag,auto_created_config_flag,cycle_count_enabled_flag,item_type,model_config_clause_name,ship_model_complete_flag,mrp_planning_code,return_inspection_requirement,ato_forecast_control,release_time_fence_code,release_time_fence_days,container_item_flag,vehicle_item_flag,
maximum_load_weight,minimum_fill_percent,container_type_code,internal_volume,wh_update_date,product_family_item_id,global_attribute_category,global_attribute1,global_attribute2,global_attribute3,global_attribute4,global_attribute5,global_attribute6,global_attribute7,global_attribute8,global_attribute9,global_attribute10,purchasing_tax_code,overcompletion_tolerance_type,overcompletion_tolerance_value,effectivity_control,check_shortages_flag,over_shipment_tolerance,under_shipment_tolerance,over_return_tolerance,under_return_tolerance,equipment_type,recovered_part_disp_code,defect_tracking_on_flag,usage_item_flag,event_flag,electronic_flag,downloadable_flag,vol_discount_exempt_flag,coupon_exempt_flag,comms_nl_trackable_flag,asset_creation_code,comms_activation_reqd_flag,orderable_on_web_flag,back_orderable_flag,web_status,indivisible_flag,dimension_uom_code,unit_length,unit_width,unit_height,bulk_picked_flag,lot_status_enabled,default_lot_status_id,serial_status_enabled,default_serial_status_id,lot_split_enabled,
lot_merge_enabled,inventory_carry_penalty,operation_slack_penalty,financing_allowed_flag,eam_item_type,eam_activity_type_code,eam_activity_cause_code,eam_act_notification_flag,eam_act_shutdown_status,dual_uom_control,secondary_uom_code,dual_uom_deviation_high,dual_uom_deviation_low,subscription_depend_flag,serv_req_enabled_code,serv_billing_enabled_flag,serv_importance_level,planned_inv_point_flag,lot_translate_enabled,default_so_source_type,create_supply_flag,substitution_window_code,substitution_window_days,ib_item_instance_class,config_model_type,lot_substitution_enabled,minimum_license_quantity,eam_activity_source_code,lifecycle_id,current_phase_id,object_version_number,tracking_quantity_ind,ont_pricing_qty_source,secondary_default_ind,option_specific_sourced,approval_status,vmi_minimum_units,vmi_minimum_days,vmi_maximum_units,vmi_maximum_days,vmi_fixed_order_quantity,so_authorization_flag,consigned_flag,asn_autoexpire_flag,vmi_forecast_type,forecast_horizon,exclude_from_budget_flag,days_tgt_inv_supply,days_tgt_inv_window,days_max_inv_supply,days_max_inv_window,drp_planned_flag,critical_component_flag,continous_transfer,convergence,divergence,config_orgs,config_match,global_attribute11,global_attribute12,global_attribute13,global_attribute14,global_attribute15,global_attribute16,global_attribute17,
global_attribute18,global_attribute19,global_attribute20,attribute16,attribute17,attribute18,attribute19,attribute20,attribute21,attribute22,attribute23,attribute24,attribute25,attribute26,attribute27,attribute28,attribute29,attribute30,cas_number,child_lot_flag,child_lot_prefix,child_lot_starting_number,child_lot_validation_flag,copy_lot_attribute_flag,default_grade,expiration_action_code,expiration_action_interval,grade_control_flag,hazardous_material_flag,hold_days,lot_divisible_flag,maturity_days,parent_child_generation_flag,process_costing_enabled_flag,process_execution_enabled_flag,process_quality_enabled_flag,process_supply_locator_id,process_supply_subinventory,process_yield_locator_id,process_yield_subinventory,recipe_enabled_flag,retest_interval,charge_periodicity_code,repair_leadtime,repair_yield,preposition_point,repair_program,subcontracting_component,outsourced_assembly,ego_master_items_dff_ctx,gdsn_outbound_enabled_flag,trade_item_descriptor,style_item_id,style_item_flag,last_submitted_nir_id,default_material_status_id,serial_tagging_flag
from geagp_glf_mtl_system_items_b""")
mtl_systm_itm_b_vw.createOrReplaceTempView("geagp_glf_mtl_system_items_b_vw")


'''for table in khw_tablename_list:
    transformation_ctx = table.replace('.', '_') + "_ctx"
    DyF = glueContext.create_dynamic_frame.from_catalog(
        database = s3_db_name, 
        table_name = table.replace('.', '_'), 
        transformation_ctx = transformation_ctx
    )
    df = DyF.toDF()
    df = sqlContext.read.parquet(khw_s3_source + table.replace('.', '_'))
    df.createOrReplaceTempView(table.replace('.', '_'))'''

s3_db_names = {"khw":"snkhp_geagp_khw"}

for table in khw_tablename_list:
     transformation_ctx = table + "_ctx"
     DyF = glueContext.create_dynamic_frame.from_catalog(
         database = s3_db_names['khw'], 
         table_name = table, 
         transformation_ctx = transformation_ctx
     )
     df = DyF.toDF()
     df.createOrReplaceTempView("geagp_khw_" +table)

wip_op_vw = spark.sql("""SELECT wip_entity_id,operation_seq_num,organization_id,repetitive_schedule_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,request_id,
program_application_id,program_id,program_update_date,operation_sequence_id,standard_operation_id,department_id,description,scheduled_quantity,quantity_in_queue,
quantity_running,quantity_waiting_to_move,quantity_rejected,quantity_scrapped,quantity_completed,first_unit_start_date,first_unit_completion_date,last_unit_start_date,last_unit_completion_date,
previous_operation_seq_num,next_operation_seq_num,count_point_type,backflush_flag,minimum_transfer_quantity,date_last_moved,attribute_category,attribute1,attribute2,attribute3,attribute4,attribute5,
attribute6,attribute7,attribute8,attribute9,attribute10,attribute11,attribute12,attribute13,attribute14,attribute15,wf_itemtype,wf_itemkey,operation_yield,operation_yield_enabled,
pre_split_quantity,operation_completed,shutdown_type,x_pos,y_pos,previous_operation_seq_id,skip_flag,long_description,cumulative_scrap_quantity,disable_date,
recommended,progress_percentage,wsm_op_seq_num,wsm_bonus_quantity,employee_id,actual_start_date,actual_completion_date,projected_completion_date,wsm_update_quantity_txn_id,wsm_costed_quantity_completed,
lowest_acceptable_yield,check_skill FROM geagp_khw_wip_operations""")
wip_op_vw.createOrReplaceTempView("geagp_khw_wip_operations_vw")

wip_ent_vw = spark.sql("""SELECT wip_entity_id,organization_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,request_id,program_application_id,
program_id,program_update_date,wip_entity_name,entity_type,description,primary_item_id,gen_object_id
from geagp_khw_wip_entities""")
wip_ent_vw.createOrReplaceTempView("geagp_khw_wip_entities_vw")

mtl_parm_vw = spark.sql("""SELECT organization_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,organization_code,master_organization_id,primary_cost_method,cost_organization_id,
default_material_cost_id,calendar_exception_set_id,calendar_code,general_ledger_update_code,default_atp_rule_id,default_picking_rule_id,default_locator_order_value,default_subinv_order_value,negative_inv_receipt_code,stock_locator_control_code,material_account,material_overhead_account,
matl_ovhd_absorption_acct,resource_account,purchase_price_var_account,ap_accrual_account,overhead_account,outside_processing_account,intransit_inv_account,interorg_receivables_account,interorg_price_var_account,interorg_payables_account,cost_of_sales_account,encumbrance_account,
project_cost_account,interorg_transfer_cr_account,matl_interorg_transfer_code,interorg_trnsfr_charge_percent,source_organization_id,source_subinventory,source_type,org_max_weight,org_max_weight_uom_code,org_max_volume,
org_max_volume_uom_code,serial_number_type,auto_serial_alpha_prefix,start_auto_serial_number,auto_lot_alpha_prefix,lot_number_uniqueness,lot_number_generation,lot_number_zero_padding,
lot_number_length,starting_revision,attribute_category,attribute1,attribute2,attribute3,attribute4,attribute5,attribute6,attribute7,attribute8,attribute9,attribute10,
attribute11,attribute12,attribute13,attribute14,attribute15,default_demand_class,encumbrance_reversal_flag,maintain_fifo_qty_stack_type,invoice_price_var_account,average_cost_var_account,sales_account,expense_account,
serial_number_generation,request_id,program_application_id,program_id,program_update_date,global_attribute_category,global_attribute1,global_attribute2,global_attribute3,global_attribute4,global_attribute5,
global_attribute6,global_attribute7,global_attribute8,global_attribute9,global_attribute10,global_attribute11,global_attribute12,
global_attribute13,global_attribute14,global_attribute15,global_attribute16,global_attribute17,global_attribute18,global_attribute19,global_attribute20,mat_ovhd_cost_type_id,project_reference_enabled,pm_cost_collection_enabled,project_control_level,avg_rates_cost_type_id,txn_approval_timeout_period,mo_source_required,mo_pick_confirm_required,mo_approval_timeout_action,borrpay_matl_var_account,borrpay_moh_var_account,borrpay_res_var_account,borrpay_osp_var_account,borrpay_ovh_var_account,process_enabled_flag,process_orgn_code,wsm_enabled_flag,default_cost_group_id,lpn_prefix,lpn_suffix,
lpn_starting_number,wms_enabled_flag,pregen_putaway_tasks_flag,regeneration_interval,timezone_id,max_picks_batch,default_wms_picking_rule_id,default_put_away_rule_id,default_task_assign_rule_id,default_label_comp_rule_id,default_carton_rule_id,default_cyc_count_header_id,crossdock_flag,cartonization_flag,cost_cutoff_date,enable_costing_by_category,cost_group_accounting,allocate_serial_flag,default_pick_task_type_id,default_cc_task_type_id,default_putaway_task_type_id,default_repl_task_type_id,
eam_enabled_flag,maint_organization_id,prioritize_wip_jobs,default_crossdock_subinventory,skip_task_waiting_minutes,qa_skipping_insp_flag,default_crossdock_locator_id,default_moxfer_task_type_id,default_moissue_task_type_id,default_matl_ovhd_cost_id,distributed_organization_flag,carrier_manifesting_flag,distribution_account_id,direct_shipping_allowed,default_pick_op_plan_id,
max_clusters_allowed,consigned_flag,cartonize_sales_orders,cartonize_manufacturing,defer_logical_transactions,wip_overpick_enabled,ovpk_transfer_orders_enabled,total_lpn_length,ucc_128_suffix_flag,wcs_enabled,allow_different_status,
child_lot_alpha_prefix,child_lot_number_length,child_lot_validation_flag,child_lot_zero_padding_flag,copy_lot_attribute_flag,create_lot_uom_conversion,genealogy_formula_security,parent_child_generation_flag,rules_override_lot_reservation,auto_del_alloc_flag,rfid_verif_pcnt_threshold,yard_management_enabled_flag,trading_partner_org_flag,deferred_cogs_account,default_crossdock_criteria_id,enforce_locator_alis_unq_flag,epc_generation_enabled_flag,company_prefix,company_prefix_index,commercial_govt_entity_number,labor_management_enabled_flag,default_status_id,lcm_enabled_flag,lcm_var_account,opsm_enabled_flag,allocate_lot_flag,cat_wt_account
from geagp_khw_mtl_parameters""")
mtl_parm_vw.createOrReplaceTempView("geagp_khw_mtl_parameters_vw")

mtl_system_itm_b_vw = spark.sql("""SELECT inventory_item_id,organization_id,last_update_date,last_updated_by,creation_date,created_by,last_update_login,summary_flag,enabled_flag,
start_date_active,end_date_active,description,buyer_id,accounting_rule_id,invoicing_rule_id,segment1,segment2,segment3,segment4,segment5,segment6,segment7,segment8,
segment9,segment10,segment11,segment12,segment13,segment14,segment15,segment16,segment17,segment18,segment19,segment20,attribute_category,attribute1,
attribute2,attribute3,attribute4,attribute5,attribute6,attribute7,attribute8,attribute9,attribute10,attribute11,attribute12,attribute13,attribute14,attribute15,
purchasing_item_flag,shippable_item_flag,customer_order_flag,internal_order_flag,service_item_flag,inventory_item_flag,eng_item_flag,inventory_asset_flag,purchasing_enabled_flag,customer_order_enabled_flag,internal_order_enabled_flag,so_transactions_flag,mtl_transactions_enabled_flag,stock_enabled_flag,bom_enabled_flag,build_in_wip_flag,revision_qty_control_code,item_catalog_group_id,catalog_status_flag,
returnable_flag,default_shipping_org,collateral_flag,taxable_flag,qty_rcv_exception_code,allow_item_desc_update_flag,inspection_required_flag,receipt_required_flag,market_price,hazard_class_id,rfq_required_flag,qty_rcv_tolerance,list_price_per_unit,un_number_id,price_tolerance_percent,asset_category_id,rounding_factor,unit_of_issue,enforce_ship_to_location_code,
allow_substitute_receipts_flag,allow_unordered_receipts_flag,allow_express_delivery_flag,days_early_receipt_allowed,days_late_receipt_allowed,receipt_days_exception_code,receiving_routing_id,invoice_close_tolerance,receive_close_tolerance,auto_lot_alpha_prefix,start_auto_lot_number,lot_control_code,shelf_life_code,shelf_life_days,serial_number_control_code,start_auto_serial_number,auto_serial_alpha_prefix,source_type,
source_organization_id,source_subinventory,expense_account,encumbrance_account,
restrict_subinventories_code,unit_weight,weight_uom_code,volume_uom_code,unit_volume,restrict_locators_code,location_control_code,shrinkage_rate,acceptable_early_days,planning_time_fence_code,demand_time_fence_code,lead_time_lot_size,std_lot_size,cum_manufacturing_lead_time,overrun_percentage,mrp_calculate_atp_flag,
acceptable_rate_increase,acceptable_rate_decrease,cumulative_total_lead_time,planning_time_fence_days,demand_time_fence_days,end_assembly_pegging_flag,repetitive_planning_flag,planning_exception_set,bom_item_type,pick_components_flag,replenish_to_order_flag,base_item_id,atp_components_flag,atp_flag,fixed_lead_time,variable_lead_time,wip_supply_locator_id,wip_supply_type,wip_supply_subinventory,primary_uom_code,primary_unit_of_measure,allowed_units_lookup_code,cost_of_sales_account,sales_account,default_include_in_rollup_flag,inventory_item_status_code,inventory_planning_code,planner_code,planning_make_buy_code,fixed_lot_multiplier,rounding_control_type,carrying_cost,postprocessing_lead_time,preprocessing_lead_time,full_lead_time,order_cost,mrp_safety_stock_percent,
mrp_safety_stock_code,min_minmax_quantity,max_minmax_quantity,minimum_order_quantity,fixed_order_quantity,fixed_days_supply,maximum_order_quantity,atp_rule_id,picking_rule_id,reservable_type,positive_measurement_error,negative_measurement_error,engineering_ecn_code,engineering_item_id,engineering_date,service_starting_delay,vendor_warranty_flag,serviceable_component_flag,serviceable_product_flag,
base_warranty_service_id,payment_terms_id,preventive_maintenance_flag,primary_specialist_id,secondary_specialist_id,serviceable_item_class_id,time_billable_flag,material_billable_flag,expense_billable_flag,prorate_service_flag,service_duration_period_code,service_duration,warranty_vendor_id,max_warranty_amount,response_time_period_code,response_time_value,
new_revision_code,invoiceable_item_flag,tax_code,invoice_enabled_flag,must_use_approved_vendor_flag,request_id,program_application_id,program_id,program_update_date,outside_operation_flag,outside_operation_uom_type,safety_stock_bucket_days,auto_reduce_mps,costing_enabled_flag,auto_created_config_flag,cycle_count_enabled_flag,
item_type,model_config_clause_name,ship_model_complete_flag,mrp_planning_code,return_inspection_requirement,ato_forecast_control,release_time_fence_code,release_time_fence_days,container_item_flag,vehicle_item_flag,maximum_load_weight,minimum_fill_percent,container_type_code,internal_volume,wh_update_date,
product_family_item_id,global_attribute_category,global_attribute1,global_attribute2,global_attribute3,global_attribute4,global_attribute5,global_attribute6,global_attribute7,global_attribute8,global_attribute9,
global_attribute10,coverage_schedule_id,
purchasing_tax_code,overcompletion_tolerance_type,overcompletion_tolerance_value,effectivity_control,check_shortages_flag,over_shipment_tolerance,under_shipment_tolerance,over_return_tolerance,under_return_tolerance,equipment_type,recovered_part_disp_code,defect_tracking_on_flag,usage_item_flag,event_flag,electronic_flag,downloadable_flag,vol_discount_exempt_flag,coupon_exempt_flag,comms_nl_trackable_flag,asset_creation_code,comms_activation_reqd_flag,orderable_on_web_flag,back_orderable_flag,web_status,indivisible_flag,dimension_uom_code,unit_length,unit_width,unit_height,bulk_picked_flag,lot_status_enabled,default_lot_status_id,
serial_status_enabled,default_serial_status_id,lot_split_enabled,lot_merge_enabled,inventory_carry_penalty,operation_slack_penalty,financing_allowed_flag,eam_item_type,eam_activity_type_code,eam_activity_cause_code,eam_act_notification_flag,eam_act_shutdown_status,dual_uom_control,secondary_uom_code,dual_uom_deviation_high,dual_uom_deviation_low,contract_item_type_code,subscription_depend_flag,serv_req_enabled_code,serv_billing_enabled_flag,serv_importance_level,planned_inv_point_flag,lot_translate_enabled,default_so_source_type,create_supply_flag,substitution_window_code,substitution_window_days,ib_item_instance_class,config_model_type,lot_substitution_enabled,minimum_license_quantity,eam_activity_source_code,lifecycle_id,current_phase_id,object_version_number,tracking_quantity_ind,
ont_pricing_qty_source,secondary_default_ind,option_specific_sourced,approval_status,vmi_minimum_units,vmi_minimum_days,vmi_maximum_units,vmi_maximum_days,vmi_fixed_order_quantity,so_authorization_flag,consigned_flag,asn_autoexpire_flag,vmi_forecast_type,forecast_horizon,exclude_from_budget_flag,days_tgt_inv_supply,days_tgt_inv_window,days_max_inv_supply,days_max_inv_window,drp_planned_flag,critical_component_flag,continous_transfer,convergence,divergence,config_orgs,config_match,attribute16,attribute17,
attribute18,attribute19,attribute20,attribute21,attribute22,attribute23,attribute24,attribute25,attribute26,attribute27,attribute28,attribute29,attribute30,cas_number,child_lot_flag,child_lot_prefix,child_lot_starting_number,child_lot_validation_flag,copy_lot_attribute_flag,default_grade,
expiration_action_code,expiration_action_interval,grade_control_flag,hazardous_material_flag,hold_days,lot_divisible_flag,maturity_days,parent_child_generation_flag,process_costing_enabled_flag,process_execution_enabled_flag,process_quality_enabled_flag,process_supply_locator_id,process_supply_subinventory,process_yield_locator_id,process_yield_subinventory,recipe_enabled_flag,retest_interval,charge_periodicity_code,repair_leadtime,repair_yield,preposition_point,repair_program,subcontracting_component,outsourced_assembly,ego_master_items_dff_ctx,gdsn_outbound_enabled_flag,trade_item_descriptor,style_item_id,style_item_flag,last_submitted_nir_id,default_material_status_id,global_attribute11,global_attribute12,global_attribute13,global_attribute14,global_attribute15,global_attribute16,global_attribute17,global_attribute18,global_attribute19,global_attribute20,serial_tagging_flag
from geagp_khw_mtl_system_items_b """)
mtl_system_itm_b_vw.createOrReplaceTempView("geagp_khw_mtl_system_items_b_vw")   

df_auto = spark.sql("""select site_code, part_num, op_min, op_max, description, description_l2, op_min4, 
                       op_max4, oper_group, master_router, last_60days_op, resource_code from geagp_ia_ia_cell_oper_lkp""")

conn = pg8000.connect(database=db_name, host=host, port=port, user=db_user, password=password, ssl=True)
cur = conn.cursor()
query = 'TRUNCATE ' + target_table
cur.execute(query)
conn.commit()
cur.close()
conn.close()


df_auto.write.mode('append') \
       .format("jdbc") \
       .option("url", "jdbc:postgresql://"+host+":"+str(port)+"/"+db_name) \
       .option("dbtable", target_table) \
       .option("user", db_user) \
       .option("password", password) \
       .save()
df_auto.write.mode('overwrite').parquet(s3_target + target_file)

df_glf = spark.sql("""select case when mtl.segment1 = 'OBFUSCATED' THEN mtl.inventory_item_id ELSE mtl.segment1 END AS segment1,
mtl.organization_id, para.organization_code, mtl.planner_code, mtl.inventory_item_id, bom.routing_sequence_id, bom_op.operation_seq_num,
op_res.usage_rate_or_amount AS oracle_standard_time, bom_op.department_id, op_res.resource_seq_num, op_res.resource_id, res.resource_code,
res.description from geagp_glf_mtl_system_items_b_vw mtl
LEFT JOIN geagp_glf_mtl_parameters para ON mtl.organization_id = para.organization_id
LEFT JOIN geagp_glf_bom_operational_routings bom ON mtl.inventory_item_id = bom.assembly_item_id and bom.organization_id = mtl.organization_id
LEFT JOIN geagp_glf_bom_operation_sequences bom_op ON bom_op.routing_sequence_id = bom.routing_sequence_id
LEFT JOIN geagp_glf_bom_operation_resources op_res ON op_res.operation_sequence_id = bom_op.operation_sequence_id
LEFT JOIN geagp_glf_bom_resources res ON res.resource_id = op_res.resource_id and res.organization_id = mtl.organization_id
WHERE mtl.organization_id IN (1088, 1089, 1090, 1048) AND bom_op.disable_date IS NULL AND op_res.resource_seq_num = 10
ORDER BY mtl.inventory_item_id, bom_op.operation_seq_num""")
df_glf.createOrReplaceTempView("geagp_ia_glf_master_route_vw")


df1 = spark.sql("""select distinct site_code, trim(part_num)as part_num, op_min, op_max, trim(description)as description, trim(description_l2)as description_l2,
                   op_min4, op_max4, oper_group, null as master_router, null as last_60days_op, null as resource_code from geagp_ia_ia_cell_oper_lkp where site_code is not null""")
df1.createOrReplaceTempView("cell_oper_lkp")

df2 = spark.sql("""select site_code, trim(part_num)as part_num, cast(null as int)as op_min, cast(null as int)as op_max, trim(oper_num)as description, cast(null as int)as description_l2,
                   cast(oper_num as int) - 1 as op_min4, cast(oper_num as int) as op_max4, null as oper_group, null as master_router, null as last_60days_op, null as resource_code 
                   from geagp_csf_rt_ca_part_oper_control poc
                   where NOT EXISTS
                   (select site_code, part_num, op_min4, op_max4 from cell_oper_lkp co
                   where co.site_code = poc.site_code and trim(co.part_num) = trim(poc.part_num) and co.op_max4 = cast(poc.oper_num as int))""")
df2.createOrReplaceTempView("op_control")

df3 = spark.sql("""select a.site_code, a.part_num, a.op_min, a.op_max, a.description, d.description_l2, a.op_min4, a.op_max4, a.oper_group, a.master_router, a.last_60days_op, a.resource_code 
                   from op_control a, cell_oper_lkp d
                   where a.site_code = d.site_code and a.op_max4 = d.op_max4 and a.op_min4 = d.op_min4""")
df3.createOrReplaceTempView("part_op_control")

df5 = spark.sql("""select t.site_code, t.part_num, t.description, t.OP_MIN4, t.OP_MAX4, t.op_min, t.op_max, t.description_l2, t.oper_group, t.master_router, t.last_60days_op, t.resource_code from
                   (select distinct 'PAR' as site_code, par.ge_part_number as part_num, par.process_id as description,
                   par.process_id - 1 AS op_min4, par.process_id as op_max4, par.process_desc as description_l2, null as op_min, null as op_max,
                   null as oper_group, null as master_router, null as last_60days_op, null as resource_code
                   from geagp_ia_ca_paradigm_malden_wip par
                   where par.ge_part_number is not null and par.ge_part_number <> ''
                   union
                   select distinct 'PAR' as site_code, par.ge_part_number as part_num, par.process_id as description,
                   par.process_id - 1 AS op_min4, par.process_id as op_max4, par.process_desc as description_l2, null as op_min, null as op_max,
                   null as oper_group, null as master_router, null as last_60days_op, null as resource_code
                   from geagp_ia_ca_paradigm_malden_throughput par
                   where par.ge_part_number is not null and par.ge_part_number <> '') t
                   left join
                   geagp_ia_ia_cell_oper_lkp c on trim(t.part_num) = trim(c.part_num) and t.op_max4 = c.op_max4 and c.site_code = t.site_code
                   where c.op_max4 is null""")
df5.createOrReplaceTempView("paradigm_data")

df6 = spark.sql("""with glf_parts as
                   (select m.segment1, m.organization_id, m.inventory_item_id, ml.organization_code from
                   (select case when segment1 = 'OBFUSCATED' then cast(inventory_item_id as string) else trim(segment1) end as segment1, organization_id, inventory_item_id 
                   from geagp_glf_mtl_system_items_b_vw where length(trim(segment1)) < 25) m
                   inner join
                   geagp_glf_mtl_parameters_vw ml on m.organization_id = ml.organization_id
                   inner join
                   geagp_ia_ia_part_cell_lkp c on m.segment1 = c.part_number AND ml.organization_code = c.site_code),
                   glf_operations as
                   (select glf_parts.organization_code, glf_parts.segment1, woper.operation_seq_num, cast(woper.last_update_date as date) from glf_parts
                   inner join
                   geagp_glf_wip_entities_vw w on w.ORGANIZATION_ID = glf_parts.ORGANIZATION_ID AND w.PRIMARY_ITEM_ID = glf_parts.INVENTORY_ITEM_ID
                   inner join
                   geagp_glf_wip_operations_vw woper on WOPER.ORGANIZATION_ID = W.ORGANIZATION_ID AND WOPER.WIP_ENTITY_ID = W.WIP_ENTITY_ID)
                   select organization_code, segment1, operation_seq_num, last_update_date from glf_operations""")
df6.createOrReplaceTempView("glf_operations")

df7 = spark.sql("""select site_code, part_num, op_min, op_max, description, description_l2, op_min4, op_max4, organization_code, segment1, operation_seq_num, last_update_date,
                   case when date_add(cast(op.last_update_date as date), 30) >= current_date then 'Y' else 'N' end as last_60days_op 
                   from geagp_ia_ia_cell_oper_lkp co, (select * from glf_operations where organization_code in ('DCC', 'TRH', 'DCR', 'ELO')) op 
                   where op.organization_code = co.site_code and op.segment1= co.part_num and  op.operation_seq_num = co.op_max4""")
df7.createOrReplaceTempView("glf_operation")

df8 = spark.sql("""with khw_parts as
                   (select m.segment1, m.organization_id, m.inventory_item_id, ml.organization_code from geagp_khw_mtl_system_items_b_vw m
                   inner join
                   geagp_khw_mtl_parameters_vw ml ON m.ORGANIZATION_ID = ml.ORGANIZATION_ID
                   inner join
                   geagp_ia_ia_part_cell_lkp c on m.segment1 = c.part_number and ml.organization_code = c.site_code),
                   khw_operations as
                   (select khw_parts.organization_code, khw_parts.segment1, woper.operation_seq_num, cast(woper.last_update_date as date) from khw_parts
                   inner join
                   geagp_khw_wip_entities_vw w on w.ORGANIZATION_ID = khw_parts.ORGANIZATION_ID and w.PRIMARY_ITEM_ID = khw_parts.INVENTORY_ITEM_ID
                   inner join
                   geagp_khw_wip_operations_vw woper on woper.ORGANIZATION_ID = w.ORGANIZATION_ID AND woper.WIP_ENTITY_ID = w.WIP_ENTITY_ID)
                   select organization_code, segment1, operation_seq_num, last_update_date from glf_operations""")
df8.createOrReplaceTempView("khw_operations")

df9 = spark.sql("""select distinct op.organization_code as site_code, op.segment1 as part_num, op.operation_seq_num as description, op.operation_seq_num - 1 as op_min4, op.operation_seq_num as op_max4, c.last_60days_op,
                   null as oper_group, null as description_l2, null as op_min, null as op_max, null as master_router, null as resource_code
                   from
                   (SELECT ORGANIZATION_CODE, SEGMENT1, OPERATION_SEQ_NUM, LAST_UPDATE_DATE FROM glf_operations WHERE date_add(cast(LAST_UPDATE_DATE as date), 30) >= current_date) op
                   left join
                   geagp_ia_ia_cell_oper_lkp c on op.segment1 = c.part_num and op.operation_seq_num = c.op_max4 and op.organization_code = c.site_code
                   where c.op_max4 is null
                   union all
                   select distinct op.organization_code as site_code, op.segment1 as part_num, op.operation_seq_num as description, op.operation_seq_num - 1 as op_min4, op.operation_seq_num as op_max4, null as last_60days_op,
                   null as oper_group, null as description_l2, null as op_min, null as op_max, null as master_router, null as resource_code
                   from
                   (SELECT ORGANIZATION_CODE, SEGMENT1, OPERATION_SEQ_NUM, LAST_UPDATE_DATE FROM khw_operations WHERE date_add(cast(LAST_UPDATE_DATE as date), 30) >= current_date) op
                   left join
                   geagp_ia_ia_cell_oper_lkp c on op.segment1 = c.part_num and op.operation_seq_num = c.op_max4 and op.organization_code = c.site_code
                   where c.op_max4 is null""")
df9.createOrReplaceTempView("glf_khw_op")

df10 = spark.sql("""select site_code, part_num, op_min, op_max, description, description_l2, op_min4, op_max4, oper_group, master_router, last_60days_op, resource_code from cell_oper_lkp
                    union
                    select site_code, part_num, op_min, op_max, description, description_l2, op_min4, op_max4, oper_group, master_router, last_60days_op, resource_code from part_op_control
                    union
                    select site_code, part_num, op_min, op_max, description, description_l2, op_min4, op_max4, oper_group, master_router, last_60days_op, resource_code from paradigm_data
                    union
                    select site_code, part_num, op_min, op_max, description, description_l2, op_min4, op_max4, oper_group, master_router, last_60days_op, resource_code from glf_khw_op""")
df10.createOrReplaceTempView("combined_df")
df_op = spark.sql("""select co.site_code, co.part_num, co.op_min, co.op_max, co.description, co.description_l2, co.op_min4, co.op_max4, co.oper_group, co.master_router, op.last_60days_op, co.resource_code from combined_df co
                     left join
                     glf_operation op ON op.organization_code = co.site_code and op.segment1= co.part_num and  op.operation_seq_num = co.op_max4""")
df_op.createOrReplaceTempView("fin_df")

df11 = spark.sql("""select op.site_code, op.part_num, op.op_min, op.op_max, op.description, op.description_l2, op.op_min4, op.op_max4, op.oper_group, op.last_60days_op, 
                    mr.master_router, mr.resource_code from fin_df op,
                    (select op.site_code, op.part_num, op.op_max4, op.description, case when mv.operation_seq_num is not null then 'Y' else 'N' end as master_router, mv.resource_code
                    from geagp_ia_ia_cell_oper_lkp op
                    left join 
                    geagp_ia_glf_master_route_vw mv on op.site_code = mv.organization_code and mv.segment1 = op.part_num and op.description = mv.operation_seq_num) mr where op.site_code = mr.site_code and op.part_num = mr.part_num and op.op_max4 = mr.op_max4""")
df11.createOrReplaceTempView("final_df")


data = spark.sql("""select distinct site_code, part_num, cast(op_min as int), cast(op_max as int), description, description_l2, cast(op_min4 as int), 
                    cast(op_max4 as int), oper_group, last_60days_op, master_router, resource_code from final_df""")

conn = pg8000.connect(database=db_name, host=host, port=port, user=db_user, password=password, ssl=True)
cur = conn.cursor()
query1 = 'TRUNCATE ' + target_table1
cur.execute(query1)
conn.commit()
cur.close()
conn.close()

data.write.mode('append') \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://"+host+":"+str(port)+"/"+db_name) \
    .option("dbtable", target_table1) \
    .option("user", db_user) \
    .option("password", password) \
    .save()

data.write.mode('append').parquet(s3_target_history + target_file1 + '/' + dt)    


DyF = DynamicFrame.fromDF(data, glueContext, "DyF")

obj_list = s3.list_objects(Bucket='med-av-daas-preprod-ca', Prefix="S3-base/geagp-ia/" + target_file)
if 'Contents' in obj_list:
    for obj in obj_list['Contents']:
        s3.delete_object(Bucket='med-av-daas-preprod-ca', Key=obj['Key'])

res = glueContext.write_dynamic_frame.from_catalog(
    frame = DyF,
    database = s3_db_name,
    table_name = target_file1,
    transformation_ctx = "res")
    
job.commit()

