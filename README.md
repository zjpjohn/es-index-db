# db-river-elasticsearch
Create elasticsearch index from db, and db query is configurable.

Create full index from db for elasticsearch, now support mysql. If work well for other db, you must to implement some interface!

We use an index file which a yaml format config file to manager index data come from which database, this config file define 
index name, type name, database info and so on. A config context as follow:

    order_v1:
      # When deleting some records, awalys do not physically delete, usually update a delete field to 'Y' or '1'
      # instead of physically delete. if you table have this field, you can config like it
      default_delete_field: is_deleted
      default_delete_valid_value: N
      include_type:
        - type: order_info
          default_schema: sea
          default_page_size: 240
          # We user relational data base, when creating index need a master table, and query data from relative table(named slave-table)
          # to fill data. 'master_table'.relation_field will be used primary key when creating index.
          master_table: db_order_info
          # Note: type config have many tables, every table can config schema, dbAddress. When we config master_field, if there are
          # some same name tables, it will bring about can't find right table. In the immediate future, will work a solution to the issue.
          include_table:
            - table: db_order_info
              page_size: 100
              fields: '*'
              forbid_fields: [admin_id, pay_url, city_id, bonus, money_paid]
              # relation_field which in local-table field relation with master_field of master-table
              # if this table is not a master-table, this config is indispensable
              relation_field: order_id
              query_condition: seller_id=1
            - table: db_order_goods
              # every table must be have master_field
              fields: [order_id, goods_id, goods_name, new_goods_sn, goods_number, measure_unit, goods_price, sold_price,
              sold_price_amount, activity_id, activity_group_id, activity_name]
              relation_field: order_id
              master_alias: order_goods_info
              # salve-table can be sub-master-table, like here: in table 'db_order_goods', the master field is order_id, but
              # we need goods info, we can query from 'db_goods', so 'db_order_goods' is master-table for 'db_goods'
            - table: db_goods
              fields: [goods_id, cat_id, brand_id, packing_value, oe_num, goods_quality_type, goods_img]
              # in salve-table, master_field pair with relation_field, relation_field is local-table field, master_field is
              # one field in master-table, you can assign to any one field of master-table, it default master-table.master_field.
              # if the master-table is a sub-master-table, it can be assigned by the format of 'table.field'
              master_alias: goods_info
              master_field: db_order_goods.goods_id
              relation_field: goods_id
              merge_type: single
            - table: db_warehouse
              schema: tqdb_base
              fields: warehouse_name
              master_field: warehouse_id
              relation_field: warehouse_id
              query_condition: [seller_id=1]
              merge_type: single
    
        - type: users
          master_table: db_users
          include_table:
            table: db_users
            schema: tqdb_base
            relation_field: user_id
            fields: '*'
        
This config file will create a index named order, it contain two type: order_info, user.

I will publish version 1.0 as soon as possible !!!!
