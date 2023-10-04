/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.ITest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractTestNativeTpcdsQueries
        extends AbstractTestQueryFramework
        implements ITest
{
    String storageFormat = "DWRF";

    private String testName = "";

    @BeforeMethod(alwaysRun = true)
    public void setCustomTestcaseName(Method method, Object[] testData)
    {
        this.testName = method.getName();
    }

    @Override
    public String getTestName()
    {
        return testName;
    }

    @DataProvider(name = "sessionProvider")
    public Object[][] sessionConfigProvider()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        return new Object[][] {
                new Object[] {
                        Session.builder(queryRunner.getDefaultSession())
                                .setSchema("tpcds")
                                .setSystemProperty(QUERY_MAX_RUN_TIME, "2m")
                                .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "2m").build()}};
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        Session session = Session.builder(queryRunner.getDefaultSession())
                .setSchema("tpcds")
                .setSystemProperty(QUERY_MAX_RUN_TIME, "2m")
                .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "2m")
                .build();

        createTpcdsCallCenter(queryRunner, session, storageFormat);
        createTpcdsCatalogPage(queryRunner, session);
        createTpcdsCatalogReturns(queryRunner, session);
        createTpcdsCatalogSales(queryRunner, session);
        createTpcdsCustomer(queryRunner, session);
        createTpcdsCustomerAddress(queryRunner, session);
        createTpcdsCustomerDemographics(queryRunner, session);
        createTpcdsDateDim(queryRunner, session, storageFormat);
        createTpcdsHouseholdDemographics(queryRunner, session);
        createTpcdsIncomeBand(queryRunner, session);
        createTpcdsInventory(queryRunner, session);
        createTpcdsItem(queryRunner, session, storageFormat);
        createTpcdsPromotion(queryRunner, session);
        createTpcdsReason(queryRunner, session);
        createTpcdsShipMode(queryRunner, session);
        createTpcdsStore(queryRunner, session, storageFormat);
        createTpcdsStoreReturns(queryRunner, session);
        createTpcdsStoreSales(queryRunner, session);
        createTpcdsTimeDim(queryRunner, session);
        createTpcdsWarehouse(queryRunner, session);
        createTpcdsWebPage(queryRunner, session, storageFormat);
        createTpcdsWebReturns(queryRunner, session);
        createTpcdsWebSales(queryRunner, session);
        createTpcdsWebSite(queryRunner, session, storageFormat);
    }

    private static void createTpcdsCallCenter(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "call_center")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE call_center AS " +
                            "SELECT cc_call_center_sk, cast(cc_call_center_id as varchar) as cc_call_center_id, cc_rec_start_date, cc_rec_end_date, " +
                            "   cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cast(cc_hours as varchar) as cc_hours, " +
                            "   cc_manager, cc_mkt_id, cast(cc_mkt_class as varchar) as cc_mkt_class, cc_mkt_desc, cc_market_manager,  " +
                            "   cc_division, cc_division_name, cc_company, cast(cc_company_name as varchar) as cc_company_name," +
                            "   cast(cc_street_number as varchar ) as cc_street_number, cc_street_name, cast(cc_street_type as varchar) as cc_street_type, " +
                            "   cast(cc_suite_number as varchar) as cc_suite_number, cc_city, cc_county, cast(cc_state as varchar) as cc_state, " +
                            "   cast(cc_zip as varchar) as cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage " +
                            "FROM tpcds.tiny.call_center");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE call_center AS " +
                            "SELECT cc_call_center_sk, cast(cc_call_center_id as varchar) as cc_call_center_id, cast(cc_rec_start_date as varchar) as cc_rec_start_date, " +
                            "   cast(cc_rec_end_date as varchar) as cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft," +
                            "   cast(cc_hours as varchar) as cc_hours, cc_manager, cc_mkt_id, cast(cc_mkt_class as varchar) as cc_mkt_class, cc_mkt_desc, cc_market_manager, " +
                            "   cc_division, cc_division_name, cc_company, cast(cc_company_name as varchar) as cc_company_name," +
                            "   cast(cc_street_number as varchar ) as cc_street_number, cc_street_name, cast(cc_street_type as varchar) as cc_street_type, " +
                            "   cast(cc_suite_number as varchar) as cc_suite_number, cc_city, cc_county, cast(cc_state as varchar) as cc_state, " +
                            "   cast(cc_zip as varchar) as cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage " +
                            "FROM tpcds.tiny.call_center");
                    break;
            }
        }
    }

    private static void createTpcdsCatalogPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_page")) {
            queryRunner.execute(session, "CREATE TABLE catalog_page AS " +
                    "SELECT cp_catalog_page_sk, cast(cp_catalog_page_id as varchar) as cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, " +
                    "   cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type " +
                    "FROM tpcds.tiny.catalog_page");
        }
    }

    private static void createTpcdsCatalogReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_returns")) {
            queryRunner.execute(session, "CREATE TABLE catalog_returns AS SELECT * FROM tpcds.tiny.catalog_returns");
        }
    }

    private static void createTpcdsCatalogSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_sales")) {
            queryRunner.execute(session, "CREATE TABLE catalog_sales AS SELECT * FROM tpcds.tiny.catalog_sales");
        }
    }

    private static void createTpcdsCustomer(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer")) {
            queryRunner.execute(session, "CREATE TABLE customer AS " +
                    "SELECT c_customer_sk, cast(c_customer_id as varchar) as c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, " +
                    "   c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, cast(c_salutation as varchar) as c_salutation, " +
                    "   cast(c_first_name as varchar) as c_first_name, cast(c_last_name as varchar) as c_last_name, " +
                    "   cast(c_preferred_cust_flag as varchar) as c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, " +
                    "   c_birth_country, cast(c_login as varchar) as c_login, cast(c_email_address as varchar) as c_email_address,  " +
                    "   c_last_review_date_sk " +
                    "FROM tpcds.tiny.customer");
        }
    }

    private static void createTpcdsCustomerAddress(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_address")) {
            queryRunner.execute(session, "CREATE TABLE customer_address AS " +
                    "SELECT ca_address_sk, cast(ca_address_id as varchar) as ca_address_id, cast(ca_street_number as varchar) as ca_street_number,  " +
                    "   ca_street_name, cast(ca_street_type as varchar) as ca_street_type, cast(ca_suite_number as varchar) as ca_suite_number,  " +
                    "   ca_city, ca_county, cast(ca_state as varchar) as ca_state, cast(ca_zip as varchar) as ca_zip, " +
                    "   ca_country, ca_gmt_offset, cast(ca_location_type as varchar) as ca_location_type " +
                    "FROM tpcds.tiny.customer_address");
        }
    }

    private static void createTpcdsCustomerDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_demographics")) {
            queryRunner.execute(session, "CREATE TABLE customer_demographics AS " +
                    "SELECT cd_demo_sk, cast(cd_gender as varchar) as cd_gender, cast(cd_marital_status as varchar) as cd_marital_status,  " +
                    "   cast(cd_education_status as varchar) as cd_education_status, cd_purchase_estimate,  " +
                    "   cast(cd_credit_rating as varchar) as cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count " +
                    "FROM tpcds.tiny.customer_demographics");
        }
    }

    private static void createTpcdsDateDim(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "date_dim")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                            "SELECT d_date_sk, cast(d_date_id as varchar) as d_date_id, d_date, " +
                            "   d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                            "   d_fy_quarter_seq, d_fy_week_seq, cast(d_day_name as varchar) as d_day_name, cast(d_quarter_name as varchar) as d_quarter_name, " +
                            "   cast(d_holiday as varchar) as d_holiday,  cast(d_weekend as varchar) as d_weekend, " +
                            "   cast(d_following_holiday as varchar) as d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,  " +
                            "   cast(d_current_day as varchar) as d_current_day, cast(d_current_week as varchar) as d_current_week, " +
                            "   cast(d_current_month as varchar) as d_current_month,  cast(d_current_quarter as varchar) as d_current_quarter, " +
                            "   cast(d_current_year as varchar) as d_current_year " +
                            "FROM tpcds.tiny.date_dim");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                            "SELECT d_date_sk, cast(d_date_id as varchar) as d_date_id, cast(d_date as varchar) as d_date, " +
                            "   d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                            "   d_fy_quarter_seq, d_fy_week_seq, cast(d_day_name as varchar) as d_day_name, cast(d_quarter_name as varchar) as d_quarter_name, " +
                            "   cast(d_holiday as varchar) as d_holiday,  cast(d_weekend as varchar) as d_weekend, " +
                            "   cast(d_following_holiday as varchar) as d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,  " +
                            "   cast(d_current_day as varchar) as d_current_day, cast(d_current_week as varchar) as d_current_week, " +
                            "   cast(d_current_month as varchar) as d_current_month,  cast(d_current_quarter as varchar) as d_current_quarter, " +
                            "   cast(d_current_year as varchar) as d_current_year " +
                            "FROM tpcds.tiny.date_dim");
                    break;
            }
        }
    }

    private static void createTpcdsHouseholdDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "household_demographics")) {
            queryRunner.execute(session, "CREATE TABLE household_demographics AS " +
                    "SELECT hd_demo_sk, hd_income_band_sk, cast(hd_buy_potential as varchar) as hd_buy_potential, hd_dep_count, hd_vehicle_count " +
                    "FROM tpcds.tiny.household_demographics");
        }
    }

    private static void createTpcdsIncomeBand(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "income_band")) {
            queryRunner.execute(session, "CREATE TABLE income_band AS " +
                    "SELECT * FROM tpcds.tiny.income_band");
        }
    }

    private static void createTpcdsInventory(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "inventory")) {
            queryRunner.execute(session, "CREATE TABLE inventory AS " +
                    "SELECT * FROM tpcds.tiny.inventory");
        }
    }

    private static void createTpcdsItem(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "item")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE item AS " +
                            "SELECT i_item_sk, cast(i_item_id as varchar) as i_item_id, i_rec_start_date, i_rec_end_date, " +
                            "   i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, cast(i_brand as varchar) as i_brand, " +
                            "   i_class_id,  cast(i_class as varchar) as i_class, i_category_id, cast(i_category as varchar) as i_category, i_manufact_id, " +
                            "   cast(i_manufact as varchar) as i_manufact, cast(i_size as varchar) as i_size, cast(i_formulation as varchar) as i_formulation, " +
                            "   cast(i_color as varchar) as i_color, cast(i_units as varchar) as i_units, cast(i_container as varchar) as i_container, i_manager_id, " +
                            "   cast(i_product_name as varchar) as i_product_name " +
                            "FROM tpcds.tiny.item");
                    break;
                case "DRWF":
                    queryRunner.execute(session, "CREATE TABLE item AS " +
                            "SELECT i_item_sk, cast(i_item_id as varchar) as i_item_id, cast(i_rec_start_date as varchar) as i_rec_start_date, " +
                            "   cast(i_rec_end_date as varchar) as i_rec_end_date, i_item_desc, cast(i_current_price as double) as i_current_price, " +
                            "   cast(i_wholesale_cost as double) as i_wholesale_cost, i_brand_id, cast(i_brand as varchar) as i_brand, " +
                            "   i_class_id,  cast(i_class as varchar) as i_class, i_category_id, cast(i_category as varchar) as i_category, i_manufact_id, " +
                            "   cast(i_manufact as varchar) as i_manufact, cast(i_size as varchar) as i_size, cast(i_formulation as varchar) as i_formulation, " +
                            "   cast(i_color as varchar) as i_color, cast(i_units as varchar) as i_units, cast(i_container as varchar) as i_container, i_manager_id, " +
                            "   cast(i_product_name as varchar) as i_product_name " +
                            "FROM tpcds.tiny.item");
                    break;
            }
        }
    }

    private static void createTpcdsPromotion(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "promotion")) {
            queryRunner.execute(session, "CREATE TABLE promotion AS " +
                    "SELECT p_promo_sk, cast(p_promo_id as varchar) as p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, " +
                    "   p_cost, p_response_targe, cast(p_promo_name as varchar) as p_promo_name, " +
                    "   cast(p_channel_dmail as varchar) as p_channel_dmail, cast(p_channel_email as varchar) as p_channel_email, " +
                    "   cast(p_channel_catalog as varchar) as p_channel_catalog, cast(p_channel_tv as varchar) as p_channel_tv, " +
                    "   cast(p_channel_radio as varchar) as p_channel_radio, cast(p_channel_press as varchar) as p_channel_press, " +
                    "   cast(p_channel_event as varchar) as p_channel_event, cast(p_channel_demo as varchar) as p_channel_demo, p_channel_details, " +
                    "   cast(p_purpose as varchar) as p_purpose, cast(p_discount_active as varchar) as p_discount_active " +
                    "FROM tpcds.tiny.promotion");
        }
    }

    private static void createTpcdsReason(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "reason")) {
            queryRunner.execute(session, "CREATE TABLE reason AS " +
                    "SELECT r_reason_sk, cast(r_reason_id as varchar) as r_reason_id, cast(r_reason_desc as varchar) as r_reason_desc " +
                    "FROM tpcds.tiny.reason");
        }
    }

    private static void createTpcdsShipMode(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "ship_mode")) {
            queryRunner.execute(session, "CREATE TABLE ship_mode AS " +
                    "SELECT sm_ship_mode_sk, cast(sm_ship_mode_id as varchar) as sm_ship_mode_id, cast(sm_type as varchar) as sm_type, " +
                    "   cast(sm_code as varchar) as sm_code, cast(sm_carrier as varchar) as sm_carrier, cast(sm_contract as varchar) as sm_contract " +
                    "FROM tpcds.tiny.ship_mode");
        }
    }

    private static void createTpcdsStore(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "store")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE store AS " +
                            "SELECT s_store_sk, cast(s_store_id as varchar) as s_store_id, s_rec_start_date, s_rec_end_date, " +
                            "   s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, cast(s_hours as varchar) as s_hours, " +
                            "   s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, " +
                            "   s_company_id, s_company_name, s_street_number, s_street_name, cast(s_street_type as varchar) as s_street_type, " +
                            "   cast(s_suite_number as varchar) as s_suite_number, s_city, s_county, cast(s_state as varchar ) as s_state, " +
                            "   cast(s_zip as varchar) as s_zip, s_country, s_gmt_offset, s_tax_precentage " +
                            "FROM tpcds.tiny.store");
                    break;
                case "DRWF":
                    queryRunner.execute(session, "CREATE TABLE store AS " +
                            "SELECT s_store_sk, cast(s_store_id as varchar) as s_store_id, cast(s_rec_start_date as varchar) as s_rec_start_date, " +
                            "   cast(s_rec_end_date as varchar) as s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, " +
                            "   cast(s_hours as varchar) as s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, " +
                            "   s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, " +
                            "   cast(s_street_type as varchar) as s_street_type, cast(s_suite_number as varchar) as s_suite_number, s_city, s_county, " +
                            "   cast(s_state as varchar ) as s_state, cast(s_zip as varchar) as s_zip, s_country, " +
                            "   cast(s_gmt_offset as double) as s_gmt_offset, " +
                            "   cast(s_tax_precentage as double) as s_tax_precentage " +
                            "FROM tpcds.tiny.store");
                    break;
            }
        }
    }

    private static void createTpcdsStoreReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_returns")) {
            queryRunner.execute(session, "CREATE TABLE store_returns AS " +
                    "SELECT * FROM tpcds.tiny.store_returns");
        }
    }

    private static void createTpcdsStoreSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_sales")) {
            queryRunner.execute(session, "CREATE TABLE store_sales AS " +
                    "SELECT * FROM tpcds.tiny.store_sales");
        }
    }

    private static void createTpcdsTimeDim(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "time_dim")) {
            queryRunner.execute(session, "CREATE TABLE time_dim AS " +
                    "SELECT t_time_sk, cast(t_time_id as varchar) as t_time_id, t_time, t_hour, t_minute, t_second,  " +
                    "   cast(t_am_pm as varchar) as t_am_pm, cast(t_shift as varchar) as t_shift, " +
                    "   cast(t_sub_shift as varchar) as t_sub_shift, cast(t_meal_time as varchar) as t_meal_time " +
                    "FROM tpcds.tiny.time_dim");
        }
    }

    private static void createTpcdsWarehouse(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "warehouse")) {
            queryRunner.execute(session, "CREATE TABLE warehouse AS " +
                    "SELECT w_warehouse_sk, cast(w_warehouse_id as varchar) as w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, " +
                    "   cast(w_street_number as varchar) as w_street_number, w_street_name, cast(w_street_type as varchar) as w_street_type, " +
                    "   cast(w_suite_number as varchar) as w_suite_number, w_city, w_county, cast(w_state as varchar) as w_state," +
                    "   cast(w_zip as varchar) as w_zip, w_country, w_gmt_offset " +
                    "FROM tpcds.tiny.warehouse");
        }
    }

    private static void createTpcdsWebPage(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "web_page")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE web_page AS " +
                            "SELECT wp_web_page_sk, cast(wp_web_page_id as varchar) as wp_web_page_id, wp_rec_start_date, wp_rec_end_date, " +
                            "   wp_creation_date_sk, wp_access_date_sk, cast(wp_autogen_flag as varchar) as wp_autogen_flag, wp_customer_sk, " +
                            "   wp_url, cast(wp_type as varchar) as wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count " +
                            "FROM tpcds.tiny.web_page");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE web_page AS " +
                            "SELECT wp_web_page_sk, cast(wp_web_page_id as varchar) as wp_web_page_id, cast(wp_rec_start_date as varchar) as wp_rec_start_date, " +
                            "   cast(wp_rec_end_date as varchar) as wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, " +
                            "   cast(wp_autogen_flag as varchar) as wp_autogen_flag, wp_customer_sk, wp_url, cast(wp_type as varchar) as wp_type, " +
                            "   wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count " +
                            "FROM tpcds.tiny.web_page");
                    break;
            }
        }
    }

    private static void createTpcdsWebReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_returns")) {
            queryRunner.execute(session, "CREATE TABLE web_returns AS " +
                    "SELECT * FROM tpcds.tiny.web_returns");
        }
    }

    private static void createTpcdsWebSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_sales")) {
            queryRunner.execute(session, "CREATE TABLE web_sales AS " +
                    "SELECT * FROM tpcds.tiny.web_sales");
        }
    }

    private static void createTpcdsWebSite(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "web_site")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE web_site AS " +
                            "SELECT web_site_sk, cast(web_site_id as varchar) as web_site_id, web_rec_start_date, web_rec_end_date, web_name, " +
                            "   web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, " +
                            "   web_company_id, cast(web_company_name as varchar) as web_company_name, cast(web_street_number as varchar) as web_street_number, " +
                            "   web_street_name, cast(web_street_type as varchar) as web_street_type, " +
                            "   cast(web_suite_number as varchar) as web_suite_number, web_city, web_county, cast(web_state as varchar) as web_state, " +
                            "   cast(web_zip as varchar) as web_zip, web_country, web_gmt_offset, web_tax_percentage " +
                            "FROM tpcds.tiny.web_site");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE web_site AS " +
                            "SELECT web_site_sk, cast(web_site_id as varchar) as web_site_id, cast(web_rec_start_date as varchar) as web_rec_start_date, " +
                            "   cast(web_rec_end_date as varchar) as web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, " +
                            "   web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, cast(web_company_name as varchar) as web_company_name, " +
                            "   cast(web_street_number as varchar) as web_street_number, web_street_name, cast(web_street_type as varchar) as web_street_type, " +
                            "   cast(web_suite_number as varchar) as web_suite_number, web_city, web_county, cast(web_state as varchar) as web_state, " +
                            "   cast(web_zip as varchar) as web_zip, web_country, cast(web_gmt_offset as double) as web_gmt_offset, " +
                            "   cast(web_tax_percentage as double) as web_tax_percentage " +
                            "FROM tpcds.tiny.web_site");
                    break;
            }
        }
    }

    protected static String getTpcdsQuery(String q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpcds/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceAll("\\$\\{database\\}\\.\\$\\{schema\\}\\.", "");
        return sql;
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ1(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("01"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ2(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("02"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ3(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("03"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ4(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("04"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ5(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("05"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ6(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("06"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ7(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("07"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ8(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("08"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ9(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("09"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ10(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("10"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ11(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("11"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ12(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("12"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ13(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("13"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ14_1(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("14_1"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ14_2(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("14_2"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ15(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("15"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ16(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("16"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ17(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("17"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ18(String propChange, Session session)
            throws Exception
    {
        // Results not equal:
        // Actual rows (up to 100 of 0 extra rows shown, 0 rows in total):
        //
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [null, null, null, null, null, null, null, null, null, null, null]
        assertQuerySucceeds(session, getTpcdsQuery("18"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ19(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("19"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ20(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("20"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ21(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("21"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ22(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("22"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ23_1(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("23_1"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ23_2(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("23_2"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ24_1(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("24_1"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ24_2(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("24_2"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ25(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("25"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ26(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("26"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ27(String propChange, Session session)
            throws Exception
    {
        // Results not equal
        // Actual rows (up to 100 of 0 extra rows shown, 0 rows in total):
        //
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [null, null, 1, null, null, null, null]
        assertQuerySucceeds(session, getTpcdsQuery("27"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ28(String propChange, Session session)
            throws Exception
    {
        // Results not equal
        // Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
        //    [77.93, 1468, 1468, 69.55, 1518, 1518, 134.06, 1167, 1167, 81.56, 1258, 1258, 60.27, 1523, 1523, 38.99, 1322, 1322]
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [77.93, 1468, 1345, 69.55, 1518, 1331, 134.06, 1167, 1107, 81.56, 1258, 1158, 60.27, 1523, 1342, 38.99, 1322, 1152]
        assertQuerySucceeds(session, getTpcdsQuery("28"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ29(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("29"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ30(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("30"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ31(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("31"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ32(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("32"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ33(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("33"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ34(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("34"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ35(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("35"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ36(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("36"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ37(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("37"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ38(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("38"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ39_1(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("39_1"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ39_2(Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("39_2"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ40(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("40"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ41(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("41"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ42(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("42"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ43(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("43"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ44(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("44"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ45(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("45"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ46(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("46"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ47(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("47"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ48(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("48"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ49(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("49"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ50(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("50"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ51(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("51"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ52(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("52"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ53(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("53"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ54(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("54"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ55(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("55"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ56(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("56"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ57(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("57"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ58(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("58"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ59(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("59"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ60(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("60"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ61(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("61"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ62(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("62"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ63(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("63"));
    }

    // TODO This test often fails in CI only. Tracked by https://github.com/prestodb/presto/issues/20271
    @Ignore
    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ64(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("64"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ65(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("65"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ66(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("66"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ67(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("67"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ68(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("68"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ69(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("69"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ70(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("70"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ71(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("71"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ72(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("72"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ73(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("73"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ74(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("74"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ75(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("75"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ76(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("76"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ77(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("77"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ78(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("78"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ79(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("79"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ80(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("80"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ81(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("81"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ82(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("82"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ83(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("83"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ84(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("84"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ85(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("85"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ86(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("86"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ87(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("87"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ88(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("88"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ89(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("89"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ90(String propChange, Session session)
            throws Exception
    {
        assertQueryFails(session, getTpcdsQuery("90"), "[\\s\\S]*Division by zero[\\s\\S]*");
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ91(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("91"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ92(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("92"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ93(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("93"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ94(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("94"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ95(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("95"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ96(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("96"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ97(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("97"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ98(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("98"));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpcdsQ99(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("99"));
    }
}
