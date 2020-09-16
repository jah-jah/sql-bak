-----------------------------------------------------
-- Export file for user OIP                        --
-- Created by Administrator on 2020/9/16, 10:20:06 --
-----------------------------------------------------

spool OIP.log

prompt
prompt Creating package P_ITSP_OIP_3MZY_BY_PL_XZ
prompt =========================================
prompt
CREATE OR REPLACE PACKAGE P_ITSP_OIP_3MZY_BY_PL_XZ IS
  -------创建日记记录变量
  V_NOWOBJECT VARCHAR2(255);
  V_COUNT     NUMBER;
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
  ----包相关变量
  V_NUM        NUMBER;
  V_CON        NUMBER(1);
  V_SQL        VARCHAR2(4000);
  v_GREATETIME Date;
  -----存储过程集合
  PROCEDURE P_OIP_IN; --存储过程入口

  PROCEDURE P_OIP_TAG; ---打标

  PROCEDURE P_OIP_BAK; ---备份

  PROCEDURE P_OIP_I; ---新增

  PROCEDURE P_OIP_D; ---删除

  PROCEDURE P_OIP_U; ---更新

  PROCEDURE P_OIP_D_TAG; ---标记及其映射射删除

END P_ITSP_OIP_3MZY_BY_PL_XZ;
/

prompt
prompt Creating procedure P_OIP_A_M_MAPPING_D
prompt ======================================
prompt
create or replace procedure P_OIP_A_M_MAPPING_D(opr number) is
-----三码资产物料映射表删除
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_MM_MAR_MAPPING_D','三码资产物料映射表删除','','',SYSDATE,NULL,'');
    COMMIT;
  v_nowobject := '三码资产物料映射表删除 P_OIP_A_M_MAPPING_D';
------头log结尾
delete from itsp.MM_ASSET_MATERIEL_MAPPING a where exists
(select 1 from  MM_ASSET_MATERIEL_MAPPING_OIP b
   where a.auto_id=b.auto_id
     and b.operation = opr
     and b.is_mv = 0 );
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错'  WHERE ID = V_SEQ;
---异常记录结束
end P_OIP_A_M_MAPPING_D;
/

prompt
prompt Creating procedure P_OIP_A_M_MAPPING_I
prompt ======================================
prompt
create or replace procedure P_OIP_A_M_MAPPING_I(opr1  number) is
---------------------异常后进行单条遍历循环
 begin
---他域主数据
for R in (select a.AUTO_ID,a.operation from MM_ASSET_MATERIEL_MAPPING_OIP a where a.operation = opr1 and a.is_mv = 0) loop
begin
----只是新增
if opr1=0 then
insert into itsp.MM_ASSET_MATERIEL_MAPPING
  (REMARK8,
   REMARK6,
   STATUS,
   ZCML,
   ZCML_NAME,
   PRDHA_NAME,
   PRDHA,
   REMARK1,
   REMARK5,
   REMARK10,
   REMARK7,
   REMARK4,
   REMARK9,
   REMARK2,
   REMARK3,
   AUTO_ID)
  select REMARK8,
         REMARK6,
         STATUS,
         ZCML,
         ZCML_NAME,
         PRDHA_NAME,
         PRDHA,
         REMARK1,
         REMARK5,
         REMARK10,
         REMARK7,
         REMARK4,
         REMARK9,
         REMARK2,
         REMARK3,
         AUTO_ID
    from MM_ASSET_MATERIEL_MAPPING_OIP a
   where a.AUTO_ID=R.AUTO_ID;
elsif opr1=1 then
-------先删除再新增
delete from itsp.MM_ASSET_MATERIEL_MAPPING a where a.AUTO_ID = R.AUTO_ID;
---------------新增
insert into itsp.MM_ASSET_MATERIEL_MAPPING
  (REMARK8,
   REMARK6,
   STATUS,
   ZCML,
   ZCML_NAME,
   PRDHA_NAME,
   PRDHA,
   REMARK1,
   REMARK5,
   REMARK10,
   REMARK7,
   REMARK4,
   REMARK9,
   REMARK2,
   REMARK3,
   AUTO_ID)
  select REMARK8,
         REMARK6,
         STATUS,
         ZCML,
         ZCML_NAME,
         PRDHA_NAME,
         PRDHA,
         REMARK1,
         REMARK5,
         REMARK10,
         REMARK7,
         REMARK4,
         REMARK9,
         REMARK2,
         REMARK3,
         AUTO_ID
    from MM_ASSET_MATERIEL_MAPPING_OIP a
   where a.AUTO_ID=R.AUTO_ID;
end if;
    commit;
---异常
EXCEPTION
    WHEN OTHERS THEN
      rollback;
insert into OIP_ERROR_DETAIL
  (table_en, id, operation, create_date)
  select 'MM_ASSET_MATERIEL_MAPPING', R.AUTO_ID, R.Operation, sysdate from dual;
 commit;
       end;
end loop;
end P_OIP_A_M_MAPPING_I;
/

prompt
prompt Creating procedure P_OIP_A_M_MAPPING_I_PL
prompt =========================================
prompt
create or replace procedure P_OIP_A_M_MAPPING_I_PL(opr number) is
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_A_M_MAPPING_I_PL','三码资产物料映射表','','',SYSDATE,NULL,'');
    COMMIT;
v_nowobject := '三码资产物料映射表 P_OIP_A_M_MAPPING_I_PL';
------头log结尾
insert into itsp.MM_ASSET_MATERIEL_MAPPING
  (REMARK8,
   REMARK6,
   STATUS,
   ZCML,
   ZCML_NAME,
   PRDHA_NAME,
   PRDHA,
   REMARK1,
   REMARK5,
   REMARK10,
   REMARK7,
   REMARK4,
   REMARK9,
   REMARK2,
   REMARK3,
   AUTO_ID)
  select REMARK8,
         REMARK6,
         STATUS,
         ZCML,
         ZCML_NAME,
         PRDHA_NAME,
         PRDHA,
         REMARK1,
         REMARK5,
         REMARK10,
         REMARK7,
         REMARK4,
         REMARK9,
         REMARK2,
         REMARK3,
         AUTO_ID
    from MM_ASSET_MATERIEL_MAPPING_OIP
   where operation = opr
     and is_mv = 0;
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
commit;
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错(批量新增异常)：有脏数据,开始单条循环遍历'  WHERE ID = V_SEQ;
       commit;
 ------游标单独新增(或者删除)
       P_OIP_A_M_MAPPING_I(opr);
---异常记录结束
end P_OIP_A_M_MAPPING_I_PL;
/

prompt
prompt Creating procedure P_OIP_DM_OMDATA_D
prompt ====================================
prompt
create or replace procedure P_OIP_DM_OMDATA_D(opr number) is
-----物料目录信息类
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_DM_OMDATA_D','他域主数据删除','','',SYSDATE,NULL,'');
    COMMIT;
  v_nowobject := '他域主数据删除 P_OIP_RM_ASSET_D';
------头log结尾
delete from itsp.DM_OMDATA a where exists
(select 1 from  DM_OMDATA_OIP b
   where a.id=b.id
     and b.operation = opr
     and b.is_mv = 0 );
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错'  WHERE ID = V_SEQ;
---异常记录结束
end P_OIP_DM_OMDATA_D;
/

prompt
prompt Creating procedure P_OIP_DM_OMDATA_I
prompt ====================================
prompt
create or replace procedure P_OIP_DM_OMDATA_I(opr1  number) is
---------------------异常后进行单条遍历循环
 begin
---他域主数据
for R in (select a.id,a.operation from DM_OMDATA_OIP a where a.operation = opr1 and a.is_mv = 0) loop
begin
----只是新增
if opr1=0 then
insert into itsp.DM_OMDATA
  (VERSION,
MODIFIER_ID,
ID,
CREATE_DATE,
TIME_STAMP,
MODIFY_DATE,
CREATOR_ID,
SPEC_ID,
SHARDING_ID)
  select VERSION,
MODIFIER_ID,
ID,
CREATE_DATE,
TIME_STAMP,
MODIFY_DATE,
CREATOR_ID,
SPEC_ID,
SHARDING_ID
    from DM_OMDATA_OIP a
   where a.id=R.ID;
elsif opr1=1 then
-------先删除再新增
delete from itsp.DM_OMDATA a where a.id = R.Id;
---------------新增
insert into itsp.DM_OMDATA
  (VERSION,
MODIFIER_ID,
ID,
CREATE_DATE,
TIME_STAMP,
MODIFY_DATE,
CREATOR_ID,
SPEC_ID,
SHARDING_ID)
  select VERSION,
MODIFIER_ID,
ID,
CREATE_DATE,
TIME_STAMP,
MODIFY_DATE,
CREATOR_ID,
SPEC_ID,
SHARDING_ID
    from DM_OMDATA_OIP a
   where a.id=R.ID;
end if;
    commit;
---异常
EXCEPTION
    WHEN OTHERS THEN
      rollback;
insert into OIP_ERROR_DETAIL
  (table_en, id, operation, create_date)
  select 'DM_OMDATA', R.Id, R.Operation, sysdate from dual;
 commit;
       end;
end loop;
end P_OIP_DM_OMDATA_I;
/

prompt
prompt Creating procedure P_OIP_DM_OMDATA_I_PL
prompt =======================================
prompt
create or replace procedure P_OIP_DM_OMDATA_I_PL(opr number) is
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_DM_OMDATA_I_PL','他域主数据','','',SYSDATE,NULL,'');
    COMMIT;
v_nowobject := '他域主数据 P_OIP_DM_OMDATA_I_PL';
------头log结尾
insert into itsp.DM_OMDATA
  (VERSION,
MODIFIER_ID,
ID,
CREATE_DATE,
TIME_STAMP,
MODIFY_DATE,
CREATOR_ID,
SPEC_ID,
SHARDING_ID)
  select VERSION,
MODIFIER_ID,
ID,
CREATE_DATE,
TIME_STAMP,
MODIFY_DATE,
CREATOR_ID,
SPEC_ID,
SHARDING_ID
    from DM_OMDATA_OIP
   where operation = opr
     and is_mv = 0;
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
commit;
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错(批量新增异常)：有脏数据,开始单条循环遍历'  WHERE ID = V_SEQ;
       commit;
 ------游标单独新增(或者删除)
       P_OIP_DM_OMDATA_I(opr);
---异常记录结束
end P_OIP_DM_OMDATA_I_PL;
/

prompt
prompt Creating procedure P_OIP_MM_ASSET_D
prompt ===================================
prompt
create or replace procedure P_OIP_MM_ASSET_D(opr number) is
-----资产目录信息类
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_MM_ASSET_D','资产目录信息类删除','','',SYSDATE,NULL,'');
    COMMIT;
  v_nowobject := '资产目录信息类删除 P_OIP_RM_ASSET_D';
------头log结尾
delete from itsp.MM_ASSET_SPEC a where exists
(select 1 from  MM_ASSET_SPEC_OIP b
   where a.id=b.id
     and b.operation = opr
     and b.is_mv = 0 );
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错'  WHERE ID = V_SEQ;
---异常记录结束
end P_OIP_MM_ASSET_D;
/

prompt
prompt Creating procedure P_OIP_MM_ASSET_I
prompt ===================================
prompt
create or replace procedure P_OIP_MM_ASSET_I(opr1  number) is
---------------------异常后进行单条遍历循环
 begin
---资产目标
for R in (select a.id,a.operation from MM_ASSET_SPEC_OIP a where a.operation = opr1 and a.is_mv = 0) loop
begin
----只是新增
if opr1=0 then
insert into itsp.MM_ASSET_SPEC
  (NAME,
   SPEC_LEVEL_ID,
   MODIFY_DATE,
   CREATOR_ID,
   MODIFIER_ID,
   NOTES,
   IS_VALID,
   ID,
   PARENT_ID,
   CREATE_DATE,
   CODE)
  select NAME,
         SPEC_LEVEL_ID,
         MODIFY_DATE,
         CREATOR_ID,
         MODIFIER_ID,
         NOTES,
         IS_VALID,
         ID,
         PARENT_ID,
         CREATE_DATE,
         CODE
    from MM_ASSET_SPEC_OIP a
   where a.id=R.ID;
elsif opr1=1 then
-------先删除再新增
delete from itsp.MM_ASSET_SPEC a where a.id = R.Id;
---------------新增
insert into itsp.MM_ASSET_SPEC
  (NAME,
   SPEC_LEVEL_ID,
   MODIFY_DATE,
   CREATOR_ID,
   MODIFIER_ID,
   NOTES,
   IS_VALID,
   ID,
   PARENT_ID,
   CREATE_DATE,
   CODE)
  select NAME,
         SPEC_LEVEL_ID,
         MODIFY_DATE,
         CREATOR_ID,
         MODIFIER_ID,
         NOTES,
         IS_VALID,
         ID,
         PARENT_ID,
         CREATE_DATE,
         CODE
    from MM_ASSET_SPEC_OIP a
   where a.id=R.ID;
end if;
    commit;
---异常
EXCEPTION
    WHEN OTHERS THEN
      rollback;
insert into OIP_ERROR_DETAIL
  (table_en, id, operation, create_date)
  select 'MM_ASSET_SPEC_OIP', R.Id, R.Operation, sysdate from dual;
 commit;
       end;
end loop;
end P_OIP_MM_ASSET_I;
/

prompt
prompt Creating procedure P_OIP_MM_ASSET_I_PL
prompt ======================================
prompt
create or replace procedure P_OIP_MM_ASSET_I_PL(opr number) is
-----资产目录新增类
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_MM_ASSET_I_PL','资产目录新增类','','',SYSDATE,NULL,'');
    COMMIT;
  v_nowobject := '资产目录新增类 P_OIP_I_RM_PROJECT';
------头log结尾
---插入工程
insert into itsp.MM_ASSET_SPEC
  (NAME,
   SPEC_LEVEL_ID,
   MODIFY_DATE,
   CREATOR_ID,
   MODIFIER_ID,
   NOTES,
   IS_VALID,
   ID,
   PARENT_ID,
   CREATE_DATE,
   CODE)
  select NAME,
         SPEC_LEVEL_ID,
         MODIFY_DATE,
         CREATOR_ID,
         MODIFIER_ID,
         NOTES,
         IS_VALID,
         ID,
         PARENT_ID,
         CREATE_DATE,
         CODE
    from MM_ASSET_SPEC_OIP a
   where a.operation = opr
     and a.is_mv = 0;
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
commit;
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错(批量新增异常)：有脏数据,开始单条循环遍历'  WHERE ID = V_SEQ;
       commit;
 ------游标单独新增(或者删除)
       P_OIP_MM_ASSET_I(opr);
---异常记录结束
end P_OIP_MM_ASSET_I_PL;
/

prompt
prompt Creating procedure P_OIP_MM_MATERIEL_D
prompt ======================================
prompt
create or replace procedure P_OIP_MM_MATERIEL_D(opr number) is
-----物料目录信息类
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_MM_MATERIEL_D','物料目录信息类删除','','',SYSDATE,NULL,'');
    COMMIT;
  v_nowobject := '物料目录信息类删除 P_OIP_RM_ASSET_D';
------头log结尾
delete from itsp.MM_MATERIEL_SPEC a where exists
(select 1 from  MM_MATERIEL_SPEC_OIP b
   where a.id=b.id
     and b.operation = opr
     and b.is_mv = 0 );
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错'  WHERE ID = V_SEQ;
---异常记录结束
end P_OIP_MM_MATERIEL_D;
/

prompt
prompt Creating procedure P_OIP_MM_MATERIEL_I
prompt ======================================
prompt
create or replace procedure P_OIP_MM_MATERIEL_I(opr1  number) is
---------------------异常后进行单条遍历循环
 begin
---物料目录信息类
for R in (select a.id,a.operation from MM_MATERIEL_SPEC_OIP a where a.operation = opr1 and a.is_mv = 0) loop
begin
----只是新增
if opr1=0 then
insert into itsp.MM_MATERIEL_SPEC
  (ID,
   PARENT_ID,
   CREATOR_ID,
   MODIFY_DATE,
   MODIFIER_ID,
   CREATE_DATE,
   IS_VALID,
   NAME,
   NOTES,
   CODE,
   SPEC_LEVEL_ID)
  select ID,
         PARENT_ID,
         CREATOR_ID,
         MODIFY_DATE,
         MODIFIER_ID,
         CREATE_DATE,
         IS_VALID,
         NAME,
         NOTES,
         CODE,
         SPEC_LEVEL_ID
    from MM_MATERIEL_SPEC_OIP a
   where a.id=R.ID;
elsif opr1=1 then
-------先删除再新增
delete from itsp.MM_MATERIEL_SPEC a where a.id = R.Id;
---------------新增
insert into itsp.MM_MATERIEL_SPEC
  (ID,
   PARENT_ID,
   CREATOR_ID,
   MODIFY_DATE,
   MODIFIER_ID,
   CREATE_DATE,
   IS_VALID,
   NAME,
   NOTES,
   CODE,
   SPEC_LEVEL_ID)
  select ID,
         PARENT_ID,
         CREATOR_ID,
         MODIFY_DATE,
         MODIFIER_ID,
         CREATE_DATE,
         IS_VALID,
         NAME,
         NOTES,
         CODE,
         SPEC_LEVEL_ID
    from MM_MATERIEL_SPEC_OIP a
   where a.id=R.ID;
end if;
    commit;
---异常
EXCEPTION
    WHEN OTHERS THEN
      rollback;
insert into OIP_ERROR_DETAIL
  (table_en, id, operation, create_date)
  select 'MM_MATERIEL_SPEC', R.Id, R.Operation, sysdate from dual;
 commit;
       end;
end loop;
end P_OIP_MM_MATERIEL_I;
/

prompt
prompt Creating procedure P_OIP_MM_MATERIEL_I_PL
prompt =========================================
prompt
create or replace procedure P_OIP_MM_MATERIEL_I_PL(opr number) is
 V_NOWOBJECT VARCHAR2(255);
 V_COUNT     NUMBER;
 V_SEQ       NUMBER;
 V_ERRORMSG VARCHAR2(255);
 begin
-----------------------日子记录----------
---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT   INTO RUN_OIP_LOG(ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES(V_SEQ,'P_OIP_MM_MATERIEL_I_PL','物料目录信息类','','',SYSDATE,NULL,'');
    COMMIT;
v_nowobject := '物料目录信息类 P_OIP_MM_MATERIEL_I_PL';
------头log结尾
---插入工程
insert into itsp.MM_MATERIEL_SPEC
  (ID,
   PARENT_ID,
   CREATOR_ID,
   MODIFY_DATE,
   MODIFIER_ID,
   CREATE_DATE,
   IS_VALID,
   NAME,
   NOTES,
   CODE,
   SPEC_LEVEL_ID)
  select ID,
         PARENT_ID,
         CREATOR_ID,
         MODIFY_DATE,
         MODIFIER_ID,
         CREATE_DATE,
         IS_VALID,
         NAME,
         NOTES,
         CODE,
         SPEC_LEVEL_ID
    from MM_MATERIEL_SPEC_OIP a
   where a.operation = opr
     and a.is_mv = 0;
----log记录
   UPDATE RUN_OIP_LOG set USETIME = (ENDDATE-STARTDATE)*60*24 WHERE ID = V_SEQ;
   UPDATE RUN_OIP_LOG SET  ENDDATE = SYSDATE WHERE ID = V_SEQ;
---- log结束
commit;
commit;
------------异常记录
EXCEPTION
    WHEN OTHERS THEN
      rollback;
      V_ERRORMSG  := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG SET USETIME = (ENDDATE-STARTDATE)*60*24,
      SQLERROR=SQLCODE||'_'||V_ERRORMSG||chr(10)||dbms_utility.format_error_backtrace(),
      COMMENTS=v_nowobject||'存过报错(批量新增异常)：有脏数据,开始单条循环遍历'  WHERE ID = V_SEQ;
       commit;
 ------游标单独新增(或者删除)
       P_OIP_MM_MATERIEL_I(opr);
---异常记录结束
end P_OIP_MM_MATERIEL_I_PL;
/

prompt
prompt Creating procedure P_OIP_RM_ASSET_D_XZ
prompt ======================================
prompt
create or replace procedure P_OIP_RM_ASSET_D_XZ(opr number) is
  -----资产信息删除
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ, 'P_OIP_RM_ASSET_D', '资产信息删除', '', '', SYSDATE, NULL, '');
  COMMIT;
  v_nowobject := '资产信息删除 P_OIP_RM_ASSET_D';
  ------头log结尾
  delete from ITSP.RM_ASSET a
   where exists (select 1
            from RM_ASSET_OIP b
           where a.id = b.id
             and b.operation = opr
             and b.is_mv = 0);
  delete from RM_ASSET@SMT a
   where exists (select 1
            from RM_ASSET_OIP b
           where a.id = b.id
             and b.operation = opr
             and b.is_mv = 0);
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (ENDDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  ---- log结束
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错'
     WHERE ID = V_SEQ;
    ---异常记录结束
end P_OIP_RM_ASSET_D_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_ASSET_I_XZ
prompt ======================================
prompt
create or replace procedure P_OIP_RM_ASSET_I_XZ(opr1 number) is
  ---------------------异常后进行单条遍历循环
begin
  ---资产
  for R in (select a.id, a.spec_id, a.operation
              from RM_ASSET_OIP a
             where a.operation = opr1
               and a.is_mv = 0) loop
    begin
      ----只是新增
      if opr1 = 0 then
        insert into ITSP.RM_ASSET
          (INACTIVEDATE,
           PURCHASEDATE,
           PURCHASEYEAR,
           FIRSTBILLING,
           ISDISABLE,
           ORIGINALASSET,
           DEVELOPASSET,
           ISRENT,
           ISLEASE,
           CREDENTIALS,
           DEMOLITIONSTATU,
           WBSELEMENT,
           USEFULMONTHS,
           SOURCEASSETNO,
           PARENT_ASSET_ID,
           ISPARENT,
           DURABLELIFE,
           ID,
           SPEC_ID,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           VERSION,
           SHARDING_ID,
           BUKRS,
           OSSZSEQ,
           ASSETSCARDCODE,
           SECONDARYASSETSCARDCODE,
           ISESTIMATE,
           ASSET_CATALOGUE,
           ASSETSTYPE,
           DESCRIPTION,
           WORKCOSTTYPE,
           NAMBERUNIT,
           PNUMBER,
           MANUFACTURER,
           STANDARD,
           ADDRESS,
           MANAGEDEPARTMENT,
           USEDEPARTMENT,
           COSTCENTER,
           SUPERVISOR,
           ASSETCUSTODIAN,
           ASSETKEEPER,
           CAPITALDATE,
           WBS,
           ASSETSRELEGATION,
           ISCLIENTASSET,
           CLIENTNAME,
           ASSETSNATURE,
           NOTES,
           ABCTYPE,
           ADDTIONREASON,
           ACCESSORY,
           PGYZ,
           PGLJZJ,
           PGLJJZ,
           ISHELDFORSALE,
           ISOVERAGE,
           ISFORSRCAP,
           ISIDLE,
           ISDEVELOPASSET,
           AREACODE,
           COUNTYOFFICES,
           BRANCH,
           BUSINESSOFFICEID,
           --BTSCODE,
           TEAM,
           ISEXPANSION,
           ISIMPAIRMENT,
           DEPRECIAERANGE1,
           DEPRECIAECODE1,
           DEPRECIAERANGE2,
           DEPRECIAECODE2,
           USEFULLIFE1,
           PERIOD1,
           USEFULLIFE2,
           PERIOD2,
           COSTVALUE,
           DEPRECIATION,
           IMPAIRMENTSUM,
           NETVALUE,
           POSITIONCODE)
          select INACTIVEDATE,
                 PURCHASEDATE,
                 PURCHASEYEAR,
                 FIRSTBILLING,
                 ISDISABLE,
                 ORIGINALASSET,
                 DEVELOPASSET,
                 ISRENT,
                 ISLEASE,
                 CREDENTIALS,
                 DEMOLITIONSTATU,
                 WBSELEMENT,
                 USEFULMONTHS,
                 SOURCEASSETNO,
                 PARENT_ASSET_ID,
                 ISPARENT,
                 DURABLELIFE,
                 ID,
                 SPEC_ID,
                 CREATOR_ID,
                 CREATE_DATE,
                 MODIFIER_ID,
                 MODIFY_DATE,
                 VERSION,
                 SHARDING_ID,
                 BUKRS,
                 OSSZSEQ,
                 ASSETSCARDCODE,
                 SECONDARYASSETSCARDCODE,
                 ISESTIMATE,
                 ASSET_CATALOGUE,
                 ASSETSTYPE,
                 DESCRIPTION,
                 WORKCOSTTYPE,
                 NAMBERUNIT,
                 PNUMBER,
                 MANUFACTURER,
                 STANDARD,
                 ADDRESS,
                 MANAGEDEPARTMENT,
                 USEDEPARTMENT,
                 COSTCENTER,
                 SUPERVISOR,
                 ASSETCUSTODIAN,
                 ASSETKEEPER,
                 CAPITALDATE,
                 WBS,
                 ASSETSRELEGATION,
                 ISCLIENTASSET,
                 CLIENTNAME,
                 ASSETSNATURE,
                 NOTES,
                 ABCTYPE,
                 ADDTIONREASON,
                 ACCESSORY,
                 PGYZ,
                 PGLJZJ,
                 PGLJJZ,
                 ISHELDFORSALE,
                 ISOVERAGE,
                 ISFORSRCAP,
                 ISIDLE,
                 ISDEVELOPASSET,
                 AREACODE,
                 COUNTYOFFICES,
                 BRANCH,
                 BUSINESSOFFICEID,
                 --BTSCODE,
                 TEAM,
                 ISEXPANSION,
                 ISIMPAIRMENT,
                 DEPRECIAERANGE1,
                 DEPRECIAECODE1,
                 DEPRECIAERANGE2,
                 DEPRECIAECODE2,
                 USEFULLIFE1,
                 PERIOD1,
                 USEFULLIFE2,
                 PERIOD2,
                 COSTVALUE,
                 DEPRECIATION,
                 IMPAIRMENTSUM,
                 NETVALUE,
                 POSITIONCODE
            from RM_ASSET_OIP a
           where a.id = R.Id;
        ----------入生产库
        insert into RM_ASSET@smt
          (INACTIVEDATE,
           PURCHASEDATE,
           PURCHASEYEAR,
           FIRSTBILLING,
           ISDISABLE,
           ORIGINALASSET,
           DEVELOPASSET,
           ISRENT,
           ISLEASE,
           CREDENTIALS,
           DEMOLITIONSTATU,
           WBSELEMENT,
           USEFULMONTHS,
           SOURCEASSETNO,
           PARENT_ASSET_ID,
           ISPARENT,
           DURABLELIFE,
           ID,
           SPEC_ID,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           VERSION,
           SHARDING_ID,
           BUKRS,
           OSSZSEQ,
           ASSETSCARDCODE,
           SECONDARYASSETSCARDCODE,
           ISESTIMATE,
           ASSET_CATALOGUE,
           ASSETSTYPE,
           DESCRIPTION,
           WORKCOSTTYPE,
           NAMBERUNIT,
           PNUMBER,
           MANUFACTURER,
           STANDARD,
           ADDRESS,
           MANAGEDEPARTMENT,
           USEDEPARTMENT,
           COSTCENTER,
           SUPERVISOR,
           ASSETCUSTODIAN,
           ASSETKEEPER,
           CAPITALDATE,
           WBS,
           ASSETSRELEGATION,
           ISCLIENTASSET,
           CLIENTNAME,
           ASSETSNATURE,
           NOTES,
           ABCTYPE,
           ADDTIONREASON,
           ACCESSORY,
           PGYZ,
           PGLJZJ,
           PGLJJZ,
           ISHELDFORSALE,
           ISOVERAGE,
           ISFORSRCAP,
           ISIDLE,
           ISDEVELOPASSET,
           AREACODE,
           COUNTYOFFICES,
           BRANCH,
           BUSINESSOFFICEID,
           --BTSCODE,
           TEAM,
           ISEXPANSION,
           ISIMPAIRMENT,
           DEPRECIAERANGE1,
           DEPRECIAECODE1,
           DEPRECIAERANGE2,
           DEPRECIAECODE2,
           USEFULLIFE1,
           PERIOD1,
           USEFULLIFE2,
           PERIOD2,
           COSTVALUE,
           DEPRECIATION,
           IMPAIRMENTSUM,
           NETVALUE,
           POSITIONCODE,
           TIME_STAMP)
          select INACTIVEDATE,
                 PURCHASEDATE,
                 PURCHASEYEAR,
                 FIRSTBILLING,
                 ISDISABLE,
                 ORIGINALASSET,
                 DEVELOPASSET,
                 ISRENT,
                 ISLEASE,
                 CREDENTIALS,
                 DEMOLITIONSTATU,
                 WBSELEMENT,
                 USEFULMONTHS,
                 SOURCEASSETNO,
                 PARENT_ASSET_ID,
                 ISPARENT,
                 DURABLELIFE,
                 ID,
                 SPEC_ID,
                 nvl(CREATOR_ID, 1),
                 nvl(CREATE_DATE, sysdate),
                 nvl(MODIFIER_ID, 1),
                 nvl(MODIFY_DATE, sysdate),
                 nvl(VERSION, 1),
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) SHARDING_ID,
                 BUKRS,
                 OSSZSEQ,
                 nvl(ASSETSCARDCODE, '未录入'),
                 SECONDARYASSETSCARDCODE,
                 ISESTIMATE,
                 ASSET_CATALOGUE,
                 ASSETSTYPE,
                 DESCRIPTION,
                 WORKCOSTTYPE,
                 NAMBERUNIT,
                 PNUMBER,
                 MANUFACTURER,
                 STANDARD,
                 ADDRESS,
                 MANAGEDEPARTMENT,
                 USEDEPARTMENT,
                 COSTCENTER,
                 SUPERVISOR,
                 ASSETCUSTODIAN,
                 ASSETKEEPER,
                 CAPITALDATE,
                 WBS,
                 ASSETSRELEGATION,
                 ISCLIENTASSET,
                 CLIENTNAME,
                 ASSETSNATURE,
                 NOTES,
                 ABCTYPE,
                 ADDTIONREASON,
                 ACCESSORY,
                 PGYZ,
                 PGLJZJ,
                 PGLJJZ,
                 ISHELDFORSALE,
                 ISOVERAGE,
                 ISFORSRCAP,
                 ISIDLE,
                 ISDEVELOPASSET,
                 AREACODE,
                 COUNTYOFFICES,
                 BRANCH,
                 BUSINESSOFFICEID,
                 --BTSCODE,
                 TEAM,
                 ISEXPANSION,
                 ISIMPAIRMENT,
                 DEPRECIAERANGE1,
                 DEPRECIAECODE1,
                 DEPRECIAERANGE2,
                 DEPRECIAECODE2,
                 USEFULLIFE1,
                 PERIOD1,
                 USEFULLIFE2,
                 PERIOD2,
                 COSTVALUE,
                 DEPRECIATION,
                 IMPAIRMENTSUM,
                 NETVALUE,
                 POSITIONCODE,
                 SYSTIMESTAMP
            from RM_ASSET_OIP a
           where a.id = R.Id;
      elsif opr1 = 1 then
        -------先删除再新增
        delete from ITSP.RM_ASSET a where a.id = R.Id;
        delete from RM_ASSET@SMT a where a.id = R.Id;
        ---------------新增
        insert into ITSP.RM_ASSET
          (INACTIVEDATE,
           PURCHASEDATE,
           PURCHASEYEAR,
           FIRSTBILLING,
           ISDISABLE,
           ORIGINALASSET,
           DEVELOPASSET,
           ISRENT,
           ISLEASE,
           CREDENTIALS,
           DEMOLITIONSTATU,
           WBSELEMENT,
           USEFULMONTHS,
           SOURCEASSETNO,
           PARENT_ASSET_ID,
           ISPARENT,
           DURABLELIFE,
           ID,
           SPEC_ID,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           VERSION,
           SHARDING_ID,
           BUKRS,
           OSSZSEQ,
           ASSETSCARDCODE,
           SECONDARYASSETSCARDCODE,
           ISESTIMATE,
           ASSET_CATALOGUE,
           ASSETSTYPE,
           DESCRIPTION,
           WORKCOSTTYPE,
           NAMBERUNIT,
           PNUMBER,
           MANUFACTURER,
           STANDARD,
           ADDRESS,
           MANAGEDEPARTMENT,
           USEDEPARTMENT,
           COSTCENTER,
           SUPERVISOR,
           ASSETCUSTODIAN,
           ASSETKEEPER,
           CAPITALDATE,
           WBS,
           ASSETSRELEGATION,
           ISCLIENTASSET,
           CLIENTNAME,
           ASSETSNATURE,
           NOTES,
           ABCTYPE,
           ADDTIONREASON,
           ACCESSORY,
           PGYZ,
           PGLJZJ,
           PGLJJZ,
           ISHELDFORSALE,
           ISOVERAGE,
           ISFORSRCAP,
           ISIDLE,
           ISDEVELOPASSET,
           AREACODE,
           COUNTYOFFICES,
           BRANCH,
           BUSINESSOFFICEID,
           --BTSCODE,
           TEAM,
           ISEXPANSION,
           ISIMPAIRMENT,
           DEPRECIAERANGE1,
           DEPRECIAECODE1,
           DEPRECIAERANGE2,
           DEPRECIAECODE2,
           USEFULLIFE1,
           PERIOD1,
           USEFULLIFE2,
           PERIOD2,
           COSTVALUE,
           DEPRECIATION,
           IMPAIRMENTSUM,
           NETVALUE,
           POSITIONCODE)
          select INACTIVEDATE,
                 PURCHASEDATE,
                 PURCHASEYEAR,
                 FIRSTBILLING,
                 ISDISABLE,
                 ORIGINALASSET,
                 DEVELOPASSET,
                 ISRENT,
                 ISLEASE,
                 CREDENTIALS,
                 DEMOLITIONSTATU,
                 WBSELEMENT,
                 USEFULMONTHS,
                 SOURCEASSETNO,
                 PARENT_ASSET_ID,
                 ISPARENT,
                 DURABLELIFE,
                 ID,
                 SPEC_ID,
                 CREATOR_ID,
                 CREATE_DATE,
                 MODIFIER_ID,
                 MODIFY_DATE,
                 VERSION,
                 SHARDING_ID,
                 BUKRS,
                 OSSZSEQ,
                 ASSETSCARDCODE,
                 SECONDARYASSETSCARDCODE,
                 ISESTIMATE,
                 ASSET_CATALOGUE,
                 ASSETSTYPE,
                 DESCRIPTION,
                 WORKCOSTTYPE,
                 NAMBERUNIT,
                 PNUMBER,
                 MANUFACTURER,
                 STANDARD,
                 ADDRESS,
                 MANAGEDEPARTMENT,
                 USEDEPARTMENT,
                 COSTCENTER,
                 SUPERVISOR,
                 ASSETCUSTODIAN,
                 ASSETKEEPER,
                 CAPITALDATE,
                 WBS,
                 ASSETSRELEGATION,
                 ISCLIENTASSET,
                 CLIENTNAME,
                 ASSETSNATURE,
                 NOTES,
                 ABCTYPE,
                 ADDTIONREASON,
                 ACCESSORY,
                 PGYZ,
                 PGLJZJ,
                 PGLJJZ,
                 ISHELDFORSALE,
                 ISOVERAGE,
                 ISFORSRCAP,
                 ISIDLE,
                 ISDEVELOPASSET,
                 AREACODE,
                 COUNTYOFFICES,
                 BRANCH,
                 BUSINESSOFFICEID,
                 --BTSCODE,
                 TEAM,
                 ISEXPANSION,
                 ISIMPAIRMENT,
                 DEPRECIAERANGE1,
                 DEPRECIAECODE1,
                 DEPRECIAERANGE2,
                 DEPRECIAECODE2,
                 USEFULLIFE1,
                 PERIOD1,
                 USEFULLIFE2,
                 PERIOD2,
                 COSTVALUE,
                 DEPRECIATION,
                 IMPAIRMENTSUM,
                 NETVALUE,
                 POSITIONCODE
            from RM_ASSET_OIP a
           where a.id = R.Id;
        ----------入生产库
        insert into RM_ASSET@smt
          (INACTIVEDATE,
           PURCHASEDATE,
           PURCHASEYEAR,
           FIRSTBILLING,
           ISDISABLE,
           ORIGINALASSET,
           DEVELOPASSET,
           ISRENT,
           ISLEASE,
           CREDENTIALS,
           DEMOLITIONSTATU,
           WBSELEMENT,
           USEFULMONTHS,
           SOURCEASSETNO,
           PARENT_ASSET_ID,
           ISPARENT,
           DURABLELIFE,
           ID,
           SPEC_ID,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           VERSION,
           SHARDING_ID,
           BUKRS,
           OSSZSEQ,
           ASSETSCARDCODE,
           SECONDARYASSETSCARDCODE,
           ISESTIMATE,
           ASSET_CATALOGUE,
           ASSETSTYPE,
           DESCRIPTION,
           WORKCOSTTYPE,
           NAMBERUNIT,
           PNUMBER,
           MANUFACTURER,
           STANDARD,
           ADDRESS,
           MANAGEDEPARTMENT,
           USEDEPARTMENT,
           COSTCENTER,
           SUPERVISOR,
           ASSETCUSTODIAN,
           ASSETKEEPER,
           CAPITALDATE,
           WBS,
           ASSETSRELEGATION,
           ISCLIENTASSET,
           CLIENTNAME,
           ASSETSNATURE,
           NOTES,
           ABCTYPE,
           ADDTIONREASON,
           ACCESSORY,
           PGYZ,
           PGLJZJ,
           PGLJJZ,
           ISHELDFORSALE,
           ISOVERAGE,
           ISFORSRCAP,
           ISIDLE,
           ISDEVELOPASSET,
           AREACODE,
           COUNTYOFFICES,
           BRANCH,
           BUSINESSOFFICEID,
           --BTSCODE,
           TEAM,
           ISEXPANSION,
           ISIMPAIRMENT,
           DEPRECIAERANGE1,
           DEPRECIAECODE1,
           DEPRECIAERANGE2,
           DEPRECIAECODE2,
           USEFULLIFE1,
           PERIOD1,
           USEFULLIFE2,
           PERIOD2,
           COSTVALUE,
           DEPRECIATION,
           IMPAIRMENTSUM,
           NETVALUE,
           POSITIONCODE,
           TIME_STAMP)
          select INACTIVEDATE,
                 PURCHASEDATE,
                 PURCHASEYEAR,
                 FIRSTBILLING,
                 ISDISABLE,
                 ORIGINALASSET,
                 DEVELOPASSET,
                 ISRENT,
                 ISLEASE,
                 CREDENTIALS,
                 DEMOLITIONSTATU,
                 WBSELEMENT,
                 USEFULMONTHS,
                 SOURCEASSETNO,
                 PARENT_ASSET_ID,
                 ISPARENT,
                 DURABLELIFE,
                 ID,
                 SPEC_ID,
                 nvl(CREATOR_ID, 1),
                 nvl(CREATE_DATE, sysdate),
                 nvl(MODIFIER_ID, 1),
                 nvl(MODIFY_DATE, sysdate),
                 nvl(VERSION, 1),
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) SHARDING_ID,
                 BUKRS,
                 OSSZSEQ,
                 nvl(ASSETSCARDCODE, '未录入'),
                 SECONDARYASSETSCARDCODE,
                 ISESTIMATE,
                 ASSET_CATALOGUE,
                 ASSETSTYPE,
                 DESCRIPTION,
                 WORKCOSTTYPE,
                 NAMBERUNIT,
                 PNUMBER,
                 MANUFACTURER,
                 STANDARD,
                 ADDRESS,
                 MANAGEDEPARTMENT,
                 USEDEPARTMENT,
                 COSTCENTER,
                 SUPERVISOR,
                 ASSETCUSTODIAN,
                 ASSETKEEPER,
                 CAPITALDATE,
                 WBS,
                 ASSETSRELEGATION,
                 ISCLIENTASSET,
                 CLIENTNAME,
                 ASSETSNATURE,
                 NOTES,
                 ABCTYPE,
                 ADDTIONREASON,
                 ACCESSORY,
                 PGYZ,
                 PGLJZJ,
                 PGLJJZ,
                 ISHELDFORSALE,
                 ISOVERAGE,
                 ISFORSRCAP,
                 ISIDLE,
                 ISDEVELOPASSET,
                 AREACODE,
                 COUNTYOFFICES,
                 BRANCH,
                 BUSINESSOFFICEID,
                 --BTSCODE,
                 TEAM,
                 ISEXPANSION,
                 ISIMPAIRMENT,
                 DEPRECIAERANGE1,
                 DEPRECIAECODE1,
                 DEPRECIAERANGE2,
                 DEPRECIAECODE2,
                 USEFULLIFE1,
                 PERIOD1,
                 USEFULLIFE2,
                 PERIOD2,
                 COSTVALUE,
                 DEPRECIATION,
                 IMPAIRMENTSUM,
                 NETVALUE,
                 POSITIONCODE,
                 SYSTIMESTAMP
            from RM_ASSET_OIP a
           where a.id = R.Id;
      end if;
      commit;
      ---异常
    EXCEPTION
      WHEN OTHERS THEN
        rollback;
        insert into OIP_ERROR_DETAIL
          (table_en, id, operation, create_date)
          select 'RM_ASSET', R.Id, R.Operation, sysdate from dual;
        commit;
    end;
  end loop;
end P_OIP_RM_ASSET_I_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_ASSET_I_PL_XZ
prompt =========================================
prompt
create or replace procedure P_OIP_RM_ASSET_I_PL_XZ(opr number) is
  -----资产卡片信息
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RM_ASSET_I_PL',
     '资产卡片新增',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '资产卡片新增 P_OIP_RM_ASSET_I_PL';
  ------头log结尾
  ---插入工程
  insert into ITSP.RM_ASSET
    (INACTIVEDATE,
     PURCHASEDATE,
     PURCHASEYEAR,
     FIRSTBILLING,
     ISDISABLE,
     ORIGINALASSET,
     DEVELOPASSET,
     ISRENT,
     ISLEASE,
     CREDENTIALS,
     DEMOLITIONSTATU,
     WBSELEMENT,
     USEFULMONTHS,
     SOURCEASSETNO,
     PARENT_ASSET_ID,
     ISPARENT,
     DURABLELIFE,
     ID,
     SPEC_ID,
     CREATOR_ID,
     CREATE_DATE,
     MODIFIER_ID,
     MODIFY_DATE,
     VERSION,
     SHARDING_ID,
     BUKRS,
     OSSZSEQ,
     ASSETSCARDCODE,
     SECONDARYASSETSCARDCODE,
     ISESTIMATE,
     ASSET_CATALOGUE,
     ASSETSTYPE,
     DESCRIPTION,
     WORKCOSTTYPE,
     NAMBERUNIT,
     PNUMBER,
     MANUFACTURER,
     STANDARD,
     ADDRESS,
     MANAGEDEPARTMENT,
     USEDEPARTMENT,
     COSTCENTER,
     SUPERVISOR,
     ASSETCUSTODIAN,
     ASSETKEEPER,
     CAPITALDATE,
     WBS,
     ASSETSRELEGATION,
     ISCLIENTASSET,
     CLIENTNAME,
     ASSETSNATURE,
     NOTES,
     ABCTYPE,
     ADDTIONREASON,
     ACCESSORY,
     PGYZ,
     PGLJZJ,
     PGLJJZ,
     ISHELDFORSALE,
     ISOVERAGE,
     ISFORSRCAP,
     ISIDLE,
     ISDEVELOPASSET,
     AREACODE,
     COUNTYOFFICES,
     BRANCH,
     BUSINESSOFFICEID,
    -- BTSCODE,
     TEAM,
     ISEXPANSION,
     ISIMPAIRMENT,
     DEPRECIAERANGE1,
     DEPRECIAECODE1,
     DEPRECIAERANGE2,
     DEPRECIAECODE2,
     USEFULLIFE1,
     PERIOD1,
     USEFULLIFE2,
     PERIOD2,
     COSTVALUE,
     DEPRECIATION,
     IMPAIRMENTSUM,
     NETVALUE,
     POSITIONCODE)
    select INACTIVEDATE,
           PURCHASEDATE,
           PURCHASEYEAR,
           FIRSTBILLING,
           ISDISABLE,
           ORIGINALASSET,
           DEVELOPASSET,
           ISRENT,
           ISLEASE,
           CREDENTIALS,
           DEMOLITIONSTATU,
           WBSELEMENT,
           USEFULMONTHS,
           SOURCEASSETNO,
           PARENT_ASSET_ID,
           ISPARENT,
           DURABLELIFE,
           ID,
           SPEC_ID,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           VERSION,
           SHARDING_ID,
           BUKRS,
           OSSZSEQ,
           ASSETSCARDCODE,
           SECONDARYASSETSCARDCODE,
           ISESTIMATE,
           ASSET_CATALOGUE,
           ASSETSTYPE,
           DESCRIPTION,
           WORKCOSTTYPE,
           NAMBERUNIT,
           PNUMBER,
           MANUFACTURER,
           STANDARD,
           ADDRESS,
           MANAGEDEPARTMENT,
           USEDEPARTMENT,
           COSTCENTER,
           SUPERVISOR,
           ASSETCUSTODIAN,
           ASSETKEEPER,
           CAPITALDATE,
           WBS,
           ASSETSRELEGATION,
           ISCLIENTASSET,
           CLIENTNAME,
           ASSETSNATURE,
           NOTES,
           ABCTYPE,
           ADDTIONREASON,
           ACCESSORY,
           PGYZ,
           PGLJZJ,
           PGLJJZ,
           ISHELDFORSALE,
           ISOVERAGE,
           ISFORSRCAP,
           ISIDLE,
           ISDEVELOPASSET,
           AREACODE,
           COUNTYOFFICES,
           BRANCH,
           BUSINESSOFFICEID,
          -- BTSCODE,
           TEAM,
           ISEXPANSION,
           ISIMPAIRMENT,
           DEPRECIAERANGE1,
           DEPRECIAECODE1,
           DEPRECIAERANGE2,
           DEPRECIAECODE2,
           USEFULLIFE1,
           PERIOD1,
           USEFULLIFE2,
           PERIOD2,
           COSTVALUE,
           DEPRECIATION,
           IMPAIRMENTSUM,
           NETVALUE,
           POSITIONCODE
      from RM_ASSET_OIP a
     where a.operation = opr
       and a.is_mv = 0;
  ----------入生产库
  insert into RM_ASSET@smt
    (INACTIVEDATE,
     PURCHASEDATE,
     PURCHASEYEAR,
     FIRSTBILLING,
     ISDISABLE,
     ORIGINALASSET,
     DEVELOPASSET,
     ISRENT,
     ISLEASE,
     CREDENTIALS,
     DEMOLITIONSTATU,
     WBSELEMENT,
     USEFULMONTHS,
     SOURCEASSETNO,
     PARENT_ASSET_ID,
     ISPARENT,
     DURABLELIFE,
     ID,
     SPEC_ID,
     CREATOR_ID,
     CREATE_DATE,
     MODIFIER_ID,
     MODIFY_DATE,
     VERSION,
     SHARDING_ID,
     BUKRS,
     OSSZSEQ,
     ASSETSCARDCODE,
     SECONDARYASSETSCARDCODE,
     ISESTIMATE,
     ASSET_CATALOGUE,
     ASSETSTYPE,
     DESCRIPTION,
     WORKCOSTTYPE,
     NAMBERUNIT,
     PNUMBER,
     MANUFACTURER,
     STANDARD,
     ADDRESS,
     MANAGEDEPARTMENT,
     USEDEPARTMENT,
     COSTCENTER,
     SUPERVISOR,
     ASSETCUSTODIAN,
     ASSETKEEPER,
     CAPITALDATE,
     WBS,
     ASSETSRELEGATION,
     ISCLIENTASSET,
     CLIENTNAME,
     ASSETSNATURE,
     NOTES,
     ABCTYPE,
     ADDTIONREASON,
     ACCESSORY,
     PGYZ,
     PGLJZJ,
     PGLJJZ,
     ISHELDFORSALE,
     ISOVERAGE,
     ISFORSRCAP,
     ISIDLE,
     ISDEVELOPASSET,
     AREACODE,
     COUNTYOFFICES,
     BRANCH,
     BUSINESSOFFICEID,
     --BTSCODE,
     TEAM,
     ISEXPANSION,
     ISIMPAIRMENT,
     DEPRECIAERANGE1,
     DEPRECIAECODE1,
     DEPRECIAERANGE2,
     DEPRECIAECODE2,
     USEFULLIFE1,
     PERIOD1,
     USEFULLIFE2,
     PERIOD2,
     COSTVALUE,
     DEPRECIATION,
     IMPAIRMENTSUM,
     NETVALUE,
     POSITIONCODE,
     TIME_STAMP)
    select INACTIVEDATE,
           PURCHASEDATE,
           PURCHASEYEAR,
           FIRSTBILLING,
           ISDISABLE,
           ORIGINALASSET,
           DEVELOPASSET,
           ISRENT,
           ISLEASE,
           CREDENTIALS,
           DEMOLITIONSTATU,
           WBSELEMENT,
           USEFULMONTHS,
           SOURCEASSETNO,
           PARENT_ASSET_ID,
           ISPARENT,
           DURABLELIFE,
           ID,
           SPEC_ID,
           nvl(CREATOR_ID, 1),
           nvl(CREATE_DATE, sysdate),
           nvl(MODIFIER_ID, 1),
           nvl(MODIFY_DATE, sysdate),
           nvl(VERSION, 1),
           (select t.id
              from t_sharding_code t
             where t.itspid = a.SHARDING_ID) SHARDING_ID,
           BUKRS,
           OSSZSEQ,
           nvl(ASSETSCARDCODE, '未录入'),
           SECONDARYASSETSCARDCODE,
           ISESTIMATE,
           ASSET_CATALOGUE,
           ASSETSTYPE,
           DESCRIPTION,
           WORKCOSTTYPE,
           NAMBERUNIT,
           PNUMBER,
           MANUFACTURER,
           STANDARD,
           ADDRESS,
           MANAGEDEPARTMENT,
           USEDEPARTMENT,
           COSTCENTER,
           SUPERVISOR,
           ASSETCUSTODIAN,
           ASSETKEEPER,
           CAPITALDATE,
           WBS,
           ASSETSRELEGATION,
           ISCLIENTASSET,
           CLIENTNAME,
           ASSETSNATURE,
           NOTES,
           ABCTYPE,
           ADDTIONREASON,
           ACCESSORY,
           PGYZ,
           PGLJZJ,
           PGLJJZ,
           ISHELDFORSALE,
           ISOVERAGE,
           ISFORSRCAP,
           ISIDLE,
           ISDEVELOPASSET,
           AREACODE,
           COUNTYOFFICES,
           BRANCH,
           BUSINESSOFFICEID,
          -- BTSCODE,
           TEAM,
           ISEXPANSION,
           ISIMPAIRMENT,
           DEPRECIAERANGE1,
           DEPRECIAECODE1,
           DEPRECIAERANGE2,
           DEPRECIAECODE2,
           USEFULLIFE1,
           PERIOD1,
           USEFULLIFE2,
           PERIOD2,
           COSTVALUE,
           DEPRECIATION,
           IMPAIRMENTSUM,
           NETVALUE,
           POSITIONCODE,
           SYSTIMESTAMP
      from RM_ASSET_OIP a
     where a.operation = opr
       and a.is_mv = 0;
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (ENDDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  commit;
  ---- log结束

  commit;
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错(批量新增异常)：有脏数据,开始单条循环遍历'
     WHERE ID = V_SEQ;
    commit;
    ------游标单独新增(或者删除)
    P_OIP_RM_ASSET_I_XZ(opr);
    ---异常记录结束
end P_OIP_RM_ASSET_I_PL_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_MATERIEL_D_XZ
prompt =========================================
prompt
create or replace procedure P_OIP_RM_MATERIEL_D_XZ(opr number) is
  -----物料信息删除
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RM_MATERIEL_D',
     '物料信息删除',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '物料信息删除 P_OIP_RM_MATERIEL_D';
  ------头log结尾
  delete from itsp.RM_MATERIEL a
   where exists (select 1
            from RM_MATERIEL_OIP b
           where a.id = b.id
             and b.operation = opr
             and b.is_mv = 0);
  delete from RM_MATERIEL@SMT b
   where exists (select 1
            from RM_MATERIEL_OIP m
           where m.id = b.id
             and m.is_mv = 0
             and m.operation = opr);
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (ENDDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  ---- log结束
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错'
     WHERE ID = V_SEQ;
    ---异常记录结束
end P_OIP_RM_MATERIEL_D_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_MATERIEL_I_XZ
prompt =========================================
prompt
create or replace procedure P_OIP_RM_MATERIEL_I_XZ(opr1 number) is
  V_ERRORMSG varchar2(255);
  ---------------------异常后进行单条遍历循环
begin
  ---插入工程
  for R in (select a.id, a.spec_id, a.operation
              from RM_MATERIEL_OIP a
             where a.operation = opr1
               and a.is_mv = 0) loop
    begin
      ----只是新增
      if opr1 = 0 then
        insert into itsp.RM_MATERIEL
          (ID,
           CREATE_DATE,
           MODIFIER_ID,
           MBLNR,
           BUDAT,
           IS_PRE_MATERIL,
           MJAHR,
           CREATOR_ID,
           PRDHA,
           BUKRS,
           MATERIEL_TYPE,
           Z_ITEXT,
           UPDATEDATE,
           SURPLUS_VALUE,
           MATCH_STATUS,
           SPEC_ID,
           BWART,
           MSEG_ZEILE,
           SHARDING_ID,
           MAKTX,
           MENGE,
           UNITPRICE,
           SHKZG,
           MODIFY_DATE,
           VERSION,
           MEINS,
           IS_ACCESSORIES,
           ZEXT_DOC_NO,
           MATNR,
           DMBTR,
           KOSTL)
          select ID,
                 CREATE_DATE,
                 MODIFIER_ID,
                 MBLNR,
                 BUDAT,
                 IS_PRE_MATERIL,
                 MJAHR,
                 CREATOR_ID,
                 PRDHA,
                 BUKRS,
                 MATERIEL_TYPE,
                 Z_ITEXT,
                 UPDATEDATE,
                 SURPLUS_VALUE,
                 MATCH_STATUS,
                 SPEC_ID,
                 BWART,
                 MSEG_ZEILE,
                 SHARDING_ID,
                 MAKTX,
                 MENGE,
                 UNITPRICE,
                 SHKZG,
                 MODIFY_DATE,
                 VERSION,
                 MEINS,
                 IS_ACCESSORIES,
                 ZEXT_DOC_NO,
                 MATNR,
                 DMBTR,
                 KOSTL
            from RM_MATERIEL_OIP a
           where a.id = R.Id;
        ----------入生产库
        INSERT INTO rm_MATERIEL@smt
          (ID,
           SPEC_ID,
           VERSION,
           MJAHR,
           MBLNR,
           ZEXT_DOC_NO,
           MSEG_ZEILE,
           PRDHA,
           MATNR,
           MAKTX,
           MENGE,
           UNITPRICE,
           DMBTR,
           BUDAT,
           BUKRS,
           UPDATEDATE,
           Z_ITEXT,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           TIME_STAMP,
           SHARDING_ID)
          SELECT a.id,
                 801102010100001,
                 nvl(VERSION, 1),
                 nvl(MJAHR, 0),
                 nvl(MBLNR, '未录入'),
                 ZEXT_DOC_NO,
                 nvl(MSEG_ZEILE, 0),
                 nvl(PRDHA, '未录入'),
                 nvl(MATNR, '未录入'),
                 MAKTX,
                 MENGE,
                 UNITPRICE,
                 DMBTR,
                 BUDAT,
                 BUKRS,
                 UPDATEDATE,
                 Z_ITEXT,
                 nvl(a.creator_id, 1),
                 nvl(a.create_date, sysdate),
                 nvl(a.modifier_id, 1),
                 nvl(a.modify_date, sysdate),
                 SYSTIMESTAMP,
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) SHARDING_ID
            FROM RM_MATERIEL_OIP A
           where A.id = R.Id;
      elsif opr1 = 1 then
        -------先删除再新增
        delete from itsp.RM_MATERIEL a where a.id = R.Id;
        delete from RM_MATERIEL@SMT b where B.id = R.Id;
        ---------------新增
        insert into itsp.RM_MATERIEL
          (ID,
           CREATE_DATE,
           MODIFIER_ID,
           MBLNR,
           BUDAT,
           IS_PRE_MATERIL,
           MJAHR,
           CREATOR_ID,
           PRDHA,
           BUKRS,
           MATERIEL_TYPE,
           Z_ITEXT,
           UPDATEDATE,
           SURPLUS_VALUE,
           MATCH_STATUS,
           SPEC_ID,
           BWART,
           MSEG_ZEILE,
           SHARDING_ID,
           MAKTX,
           MENGE,
           UNITPRICE,
           SHKZG,
           MODIFY_DATE,
           VERSION,
           MEINS,
           IS_ACCESSORIES,
           ZEXT_DOC_NO,
           MATNR,
           DMBTR,
           KOSTL)
          select ID,
                 CREATE_DATE,
                 MODIFIER_ID,
                 MBLNR,
                 BUDAT,
                 IS_PRE_MATERIL,
                 MJAHR,
                 CREATOR_ID,
                 PRDHA,
                 BUKRS,
                 MATERIEL_TYPE,
                 Z_ITEXT,
                 UPDATEDATE,
                 SURPLUS_VALUE,
                 MATCH_STATUS,
                 SPEC_ID,
                 BWART,
                 MSEG_ZEILE,
                 SHARDING_ID,
                 MAKTX,
                 MENGE,
                 UNITPRICE,
                 SHKZG,
                 MODIFY_DATE,
                 VERSION,
                 MEINS,
                 IS_ACCESSORIES,
                 ZEXT_DOC_NO,
                 MATNR,
                 DMBTR,
                 KOSTL
            from RM_MATERIEL_OIP a
           where a.id = R.Id;
        ----------入生产库
        INSERT INTO rm_MATERIEL@smt
          (ID,
           SPEC_ID,
           VERSION,
           MJAHR,
           MBLNR,
           ZEXT_DOC_NO,
           MSEG_ZEILE,
           PRDHA,
           MATNR,
           MAKTX,
           MENGE,
           UNITPRICE,
           DMBTR,
           BUDAT,
           BUKRS,
           UPDATEDATE,
           Z_ITEXT,
           CREATOR_ID,
           CREATE_DATE,
           MODIFIER_ID,
           MODIFY_DATE,
           TIME_STAMP,
           SHARDING_ID)
          SELECT a.id,
                 801102010100001,
                 nvl(VERSION, 1),
                 nvl(MJAHR, 0),
                 nvl(MBLNR, '未录入'),
                 ZEXT_DOC_NO,
                 nvl(MSEG_ZEILE, 0),
                 nvl(PRDHA, '未录入'),
                 nvl(MATNR, '未录入'),
                 MAKTX,
                 MENGE,
                 UNITPRICE,
                 DMBTR,
                 BUDAT,
                 BUKRS,
                 UPDATEDATE,
                 Z_ITEXT,
                 nvl(a.creator_id, 1),
                 nvl(a.create_date, sysdate),
                 nvl(a.modifier_id, 1),
                 nvl(a.modify_date, sysdate),
                 SYSTIMESTAMP,
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) SHARDING_ID
            FROM RM_MATERIEL_OIP A
           where A.id = R.Id;
      end if;
      commit;
      ---异常
    EXCEPTION
      WHEN OTHERS THEN
        rollback;
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        insert into OIP_ERROR_LOG
          (table_en, id, operation, create_date, sqlerror)
          select 'RM_MATERIEL_OIP',
                 R.Id,
                 R.Operation,
                 sysdate,
                 V_ERRORMSG || chr(10) ||
                 dbms_utility.format_error_backtrace()
            from dual;
        commit;
    end;
  end loop;
end P_OIP_RM_MATERIEL_I_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_MATERIEL_I_PL_XZ
prompt ============================================
prompt
create or replace procedure P_OIP_RM_MATERIEL_I_PL_XZ(opr number) is
  -----物料信息
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RM_MATERIEL_I_PL',
     '物料信息新增',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '物料信息新增 P_OIP_RM_MATERIEL_I_PL';
  ------头log结尾
  ---资产卡片新增
  insert into itsp.RM_MATERIEL
    (ID,
     CREATE_DATE,
     MODIFIER_ID,
     MBLNR,
     BUDAT,
     IS_PRE_MATERIL,
     MJAHR,
     CREATOR_ID,
     PRDHA,
     BUKRS,
     MATERIEL_TYPE,
     Z_ITEXT,
     UPDATEDATE,
     SURPLUS_VALUE,
     MATCH_STATUS,
     SPEC_ID,
     BWART,
     MSEG_ZEILE,
     SHARDING_ID,
     MAKTX,
     MENGE,
     UNITPRICE,
     SHKZG,
     MODIFY_DATE,
     VERSION,
     MEINS,
     IS_ACCESSORIES,
     ZEXT_DOC_NO,
     MATNR,
     DMBTR,
     KOSTL)
    select ID,
           CREATE_DATE,
           MODIFIER_ID,
           MBLNR,
           BUDAT,
           IS_PRE_MATERIL,
           MJAHR,
           CREATOR_ID,
           PRDHA,
           BUKRS,
           MATERIEL_TYPE,
           Z_ITEXT,
           UPDATEDATE,
           SURPLUS_VALUE,
           MATCH_STATUS,
           SPEC_ID,
           BWART,
           MSEG_ZEILE,
           SHARDING_ID,
           MAKTX,
           MENGE,
           UNITPRICE,
           SHKZG,
           MODIFY_DATE,
           VERSION,
           MEINS,
           IS_ACCESSORIES,
           ZEXT_DOC_NO,
           MATNR,
           DMBTR,
           KOSTL
      from RM_MATERIEL_OIP a
     where a.operation = opr
       and a.is_mv = 0;
  ----------入生产库
  INSERT INTO rm_MATERIEL@smt
    (ID,
     SPEC_ID,
     VERSION,
     MJAHR,
     MBLNR,
     ZEXT_DOC_NO,
     MSEG_ZEILE,
     PRDHA,
     MATNR,
     MAKTX,
     MENGE,
     UNITPRICE,
     DMBTR,
     BUDAT,
     BUKRS,
     UPDATEDATE,
     Z_ITEXT,
     CREATOR_ID,
     CREATE_DATE,
     MODIFIER_ID,
     MODIFY_DATE,
     TIME_STAMP,
     SHARDING_ID)
    SELECT a.id,
           801102010100001,
           nvl(VERSION, 1),
           nvl(MJAHR, 0),
           nvl(MBLNR, '未录入'),
           ZEXT_DOC_NO,
           nvl(MSEG_ZEILE, 0),
           nvl(PRDHA, '未录入'),
           nvl(MATNR, '未录入'),
           MAKTX,
           MENGE,
           UNITPRICE,
           DMBTR,
           BUDAT,
           BUKRS,
           UPDATEDATE,
           Z_ITEXT,
           nvl(a.creator_id, 1),
           nvl(a.create_date, sysdate),
           nvl(a.modifier_id, 1),
           nvl(a.modify_date, sysdate),
           SYSTIMESTAMP,
           (select t.id
              from t_sharding_code t
             where t.itspid = a.SHARDING_ID) SHARDING_ID
      FROM RM_MATERIEL_OIP A
     where a.is_mv = 0
       and a.operation = opr;
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  commit;
  ---- log结束
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错(批量新增异常)：有脏数据,开始单条循环遍历'
     WHERE ID = V_SEQ;
    commit;
    ------游标单独新增(或者删除)
    P_OIP_RM_MATERIEL_I_XZ(opr);
    ---异常记录结束
end P_OIP_RM_MATERIEL_I_PL_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_PROJECT_D_XZ
prompt ========================================
prompt
create or replace procedure P_OIP_RM_PROJECT_D_XZ(opr number) is
  -----工程信息删除
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RM_PROJECT_D',
     '工程信息删除',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '工程信息删除 P_OIP_RM_PROJECT_D';
  ------头log结尾
  delete from ITSP.rm_project a
   where exists (select 1
            from RM_PROJECT_OIP b
           where a.id = b.id
             and b.operation = opr
             and b.is_mv = 0);
  delete from ITSP.RE_PROJECT_MSS b
   where exists (select 1
            from RM_PROJECT_OIP a
           where a.id = b.project_id
             and a.operation = opr
             and a.is_mv = 0
             and a.spec_id = 2930100001);
  -----删除
  delete from Re_Project_Project_Ccs@smt a
   where exists (select 1
            from RM_PROJECT_OIP m
           where M.ID = a.project_id
             and m.is_mv = 0
             and m.operation = opr);
  delete from Rm_Project@smt b
   where exists (select 1
            from RM_PROJECT_OIP m
           where M.ID = b.id
             and m.is_mv = 0
             and m.operation = opr);
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  ---- log结束
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错'
     WHERE ID = V_SEQ;
    ---异常记录结束
end P_OIP_RM_PROJECT_D_XZ;
/

prompt
prompt Creating procedure P_OIP_RM_PROJECT_I_XZ
prompt ========================================
prompt
create or replace procedure P_OIP_RM_PROJECT_I_xz(opr1 number) is
  ---------------------异常后进行单条遍历循环
  V_ERRORMSG VARCHAR2(255);
begin
  ---插入工程
  for R in (select a.id, a.spec_id, a.operation
              from Rm_Project_oip a
             where a.operation = opr1
               and a.is_mv = 0) loop
    begin
      ----只是新增
      if opr1 = 0 then
        insert into ITSP.rm_project
          (id,
           spec_id,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp)
          select id,
                 spec_id,
                 creator_id,
                 create_date,
                 modifier_id,
                 modify_date,
                 version,
                 sharding_id,
                 time_stamp
            from RM_PROJECT_OIP a
           where a.id = R.ID;
        -----插入mms工程
        insert into ITSP.RE_PROJECT_MSS
          (project_id,
           no,
           name,
           projectstate,
           buildingplace,
           designnumber,
           itemmodifydate,
           itemdate,
           area_code,
           projectman,
           constructcompany,
           constructor,
           designprovider,
           designer,
           companyname,
           managelevel,
           constructproperty,
           plantype,
           specialsmalltype,
           constructtype,
           prctr,
           bukrs,
           thirdsupplier,
           sharding_id,
           is_create_project,
           forecontractcode,
           managemode,
           itemidentifying)
          select id,
                 no,
                 name,
                 projectstate,
                 buildingplace,
                 designnumber,
                 itemmodifydate,
                 itemdate,
                 area_code,
                 projectman,
                 constructcompany,
                 constructor,
                 designprovider,
                 designer,
                 companyname,
                 managelevel,
                 constructproperty,
                 plantype,
                 specialsmalltype,
                 constructtype,
                 prctr,
                 bukrs,
                 thirdsupplier,
                 sharding_id,
                 is_create_project,
                 forecontractcode,
                 managemode,
                 itemidentifying
            from RM_PROJECT_OIP a
           where a.id = R.Id
             and a.spec_id = R.Spec_Id;
        ----------入生产库
        insert into Rm_Project@smt
          (id,
           spec_id,
           REGION_ID,
           Project_Status_Id,
           --COSTCENTERCODE,
           CONTACT_CONSTRUCTION_UNIT,
           CONSTRUCTION_UNIT,
           NAME,
           DESIGNER,
           CODE,
           solid_state_engineering,
           sharding_id)
          select ID,
                 2930010001,
                 decode(a.sharding_id,
                        131000000000000001216375,
                        (case
                          when companyname like '%石家庄%' then
                           131000000000000001216376
                          when companyname like '%邯郸%' then
                           131000000000000001216383
                          when companyname like '%沧州%' then
                           131000000000000001216375
                          when companyname like '%邢台%' then
                           131000000000000001216412
                          when companyname like '%衡水%' then
                           131000000000000001216397
                          when companyname like '%保定%' then
                           131000000000000001216377
                          when companyname like '%雄安%' then
                           131000000000000001216377
                          when companyname like '%张家口%' then
                           131000000000000001216396
                          when companyname like '%承德%' then
                           131000000000000001216413
                          when companyname like '%唐山%' then
                           131000000000000001216380
                          when companyname like '%廊坊%' then
                           131000000000000001216378
                          when companyname like '%秦皇岛%' then
                           131000000000000001216379
                          else
                           131000000000000001216376
                        end),
                        a.sharding_id) regionid,
                 (select decode(n.nm_dict_value, 0, 80204667, 3, 80204668)
                    from t_itsp_nm_dict_match n
                   where n.itsp_dict_value = a.PROJECTSTATE
                     and n.nm_tab_name = 'PROJECT'
                     and n.nm_colu_name = 'STATE'),
                 CONSTRUCTOR,
                 NVL(COMPANYNAME, '未录入'),
                 NVL(NAME, '未录入'),
                 DESIGNER,
                 NVL(NO, '未录入'),
                 (select nvl(chprojstatus, '未知')
                    from de_om_v_o_cprojstatus
                   where mdata_id = a.projectstate),
                 decode(a.sharding_id,
                        131000000000000001216375,
                        (case
                          when companyname like '%石家庄%' then
                           311
                          when companyname like '%邯郸%' then
                           310
                          when companyname like '%沧州%' then
                           317
                          when companyname like '%邢台%' then
                           319
                          when companyname like '%衡水%' then
                           318
                          when companyname like '%保定%' then
                           312
                          when companyname like '%雄安%' then
                           312
                          when companyname like '%张家口%' then
                           313
                          when companyname like '%承德%' then
                           314
                          when companyname like '%唐山%' then
                           315
                          when companyname like '%廊坊%' then
                           316
                          when companyname like '%秦皇岛%' then
                           335
                          else
                           311
                        end),
                        131000000000000001216376,
                        311,
                        131000000000000001216377,
                        312,
                        131000000000000001216396,
                        313,
                        131000000000000001216413,
                        314,
                        131000000000000001216378,
                        316,
                        131000000000000001216379,
                        335,
                        131000000000000001216380,
                        315,
                        131000000000000001216383,
                        310,
                        131000000000000001216397,
                        318,
                        131000000000000001216412,
                        319,
                        13)
            from Rm_Project_oip a
           where a.id = R.Id;
        insert into Re_Project_Project_Ccs@smt
          (Project_Id,
           Is_Partner_Id,
           Sanma_Type_Id,
           Plan_Type,
           Special_Small_Type)
          select ID, 80201002, 90402827, plantype, specialsmalltype
            from Rm_Project_oip a
           where a.id = R.Id;
      elsif opr1 = 1 then
        -------先删除再新增
        delete from ITSP.rm_project a where a.id = R.Id;
        delete from ITSP.RE_PROJECT_MSS b where b.project_id = R.Id;
        ---删除
        delete from Re_Project_Project_Ccs@smt where Project_Id = r.id;
        delete from rm_project@smt b where b.id = r.id;
        ---------------新增
        insert into ITSP.rm_project
          (id,
           spec_id,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp)
          select id,
                 spec_id,
                 creator_id,
                 create_date,
                 modifier_id,
                 modify_date,
                 version,
                 sharding_id,
                 time_stamp
            from RM_PROJECT_OIP a
           where a.id = R.ID;
        insert into ITSP.RE_PROJECT_MSS
          (project_id,
           no,
           name,
           projectstate,
           buildingplace,
           designnumber,
           itemmodifydate,
           itemdate,
           area_code,
           projectman,
           constructcompany,
           constructor,
           designprovider,
           designer,
           companyname,
           managelevel,
           constructproperty,
           plantype,
           specialsmalltype,
           constructtype,
           prctr,
           bukrs,
           thirdsupplier,
           sharding_id,
           is_create_project,
           forecontractcode,
           managemode,
           itemidentifying)
          select id,
                 no,
                 name,
                 projectstate,
                 buildingplace,
                 designnumber,
                 itemmodifydate,
                 itemdate,
                 area_code,
                 projectman,
                 constructcompany,
                 constructor,
                 designprovider,
                 designer,
                 companyname,
                 managelevel,
                 constructproperty,
                 plantype,
                 specialsmalltype,
                 constructtype,
                 prctr,
                 bukrs,
                 thirdsupplier,
                 sharding_id,
                 is_create_project,
                 forecontractcode,
                 managemode,
                 itemidentifying
            from RM_PROJECT_OIP a
           where a.id = R.Id
             and a.spec_id = R.Spec_Id;
      
        ----------入生产库
        insert into Rm_Project@smt
          (id,
           spec_id,
           REGION_ID,
           Project_Status_Id,
           --COSTCENTERCODE,
           CONTACT_CONSTRUCTION_UNIT,
           CONSTRUCTION_UNIT,
           NAME,
           DESIGNER,
           CODE,
           solid_state_engineering,
           sharding_id)
          select ID,
                 2930010001,
                 decode(a.sharding_id,
                        131000000000000001216375,
                        (case
                          when companyname like '%石家庄%' then
                           131000000000000001216376
                          when companyname like '%邯郸%' then
                           131000000000000001216383
                          when companyname like '%沧州%' then
                           131000000000000001216375
                          when companyname like '%邢台%' then
                           131000000000000001216412
                          when companyname like '%衡水%' then
                           131000000000000001216397
                          when companyname like '%保定%' then
                           131000000000000001216377
                          when companyname like '%雄安%' then
                           131000000000000001216377
                          when companyname like '%张家口%' then
                           131000000000000001216396
                          when companyname like '%承德%' then
                           131000000000000001216413
                          when companyname like '%唐山%' then
                           131000000000000001216380
                          when companyname like '%廊坊%' then
                           131000000000000001216378
                          when companyname like '%秦皇岛%' then
                           131000000000000001216379
                          else
                           131000000000000001216376
                        end),
                        a.sharding_id) regionid,
                 (select decode(n.nm_dict_value, 0, 80204667, 3, 80204668)
                    from t_itsp_nm_dict_match n
                   where n.itsp_dict_value = a.PROJECTSTATE
                     and n.nm_tab_name = 'PROJECT'
                     and n.nm_colu_name = 'STATE'),
                 CONSTRUCTOR,
                 NVL(COMPANYNAME, '未录入'),
                 NVL(NAME, '未录入'),
                 DESIGNER,
                 NVL(NO, '未录入'),
                 (select nvl(chprojstatus, '未知')
                    from de_om_v_o_cprojstatus
                   where mdata_id = a.projectstate),
                 decode(a.sharding_id,
                        131000000000000001216375,
                        (case
                          when companyname like '%石家庄%' then
                           311
                          when companyname like '%邯郸%' then
                           310
                          when companyname like '%沧州%' then
                           317
                          when companyname like '%邢台%' then
                           319
                          when companyname like '%衡水%' then
                           318
                          when companyname like '%保定%' then
                           312
                          when companyname like '%雄安%' then
                           312
                          when companyname like '%张家口%' then
                           313
                          when companyname like '%承德%' then
                           314
                          when companyname like '%唐山%' then
                           315
                          when companyname like '%廊坊%' then
                           316
                          when companyname like '%秦皇岛%' then
                           335
                          else
                           311
                        end),
                        131000000000000001216376,
                        311,
                        131000000000000001216377,
                        312,
                        131000000000000001216396,
                        313,
                        131000000000000001216413,
                        314,
                        131000000000000001216378,
                        316,
                        131000000000000001216379,
                        335,
                        131000000000000001216380,
                        315,
                        131000000000000001216383,
                        310,
                        131000000000000001216397,
                        318,
                        131000000000000001216412,
                        319,
                        13)
            from Rm_Project_oip a
           where a.id = R.Id;
        insert into Re_Project_Project_Ccs@smt
          (Project_Id,
           Is_Partner_Id,
           Sanma_Type_Id,
           Plan_Type,
           Special_Small_Type)
          select ID, 80201002, 90402827, plantype, specialsmalltype
            from Rm_Project_oip a
           where a.id = R.Id;
      end if;
      commit;
      ---异常
    EXCEPTION
      WHEN OTHERS THEN
        rollback;
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        insert into OIP_ERROR_LOG
          (table_en, id, operation, create_date, sqlerror)
          select 'RM_PROJECT_OIP',
                 R.Id,
                 R.Operation,
                 sysdate,
                 V_ERRORMSG || chr(10) ||
                 dbms_utility.format_error_backtrace()
            from dual;
        commit;
    end;
  end loop;
end P_OIP_RM_PROJECT_I_xz;
/

prompt
prompt Creating procedure P_OIP_RM_PROJECT_I_PL_XZ
prompt ===========================================
prompt
create or replace procedure P_OIP_RM_PROJECT_I_PL_XZ(opr number) is
  -----工程信息
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  ---头log记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RM_PROJECT_I_PL',
     '工程信息新增',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '工程信息新增 P_OIP_RM_PROJECT_I_PL';
  ---插入工程
  insert into itsp.rm_project
    (id,
     spec_id,
     creator_id,
     create_date,
     modifier_id,
     modify_date,
     version,
     sharding_id,
     time_stamp)
    select id,
           spec_id,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp
      from RM_PROJECT_OIP a
     where a.operation = opr
       and a.is_mv = 0;
  -----插入mms工程
  insert into itsp.RE_PROJECT_MSS
    (project_id,
     no,
     name,
     projectstate,
     buildingplace,
     designnumber,
     itemmodifydate,
     itemdate,
     area_code,
     projectman,
     constructcompany,
     constructor,
     designprovider,
     designer,
     companyname,
     managelevel,
     constructproperty,
     plantype,
     specialsmalltype,
     constructtype,
     prctr,
     bukrs,
     thirdsupplier,
     sharding_id,
     is_create_project,
     forecontractcode,
     managemode,
     itemidentifying)
    select id,
           no,
           name,
           projectstate,
           buildingplace,
           designnumber,
           itemmodifydate,
           itemdate,
           area_code,
           projectman,
           constructcompany,
           constructor,
           designprovider,
           designer,
           companyname,
           managelevel,
           constructproperty,
           plantype,
           specialsmalltype,
           constructtype,
           prctr,
           bukrs,
           thirdsupplier,
           sharding_id,
           is_create_project,
           forecontractcode,
           managemode,
           itemidentifying
      from RM_PROJECT_OIP a
     where a.operation = opr
       and a.is_mv = 0
       and a.spec_id = 2930100001;
  ----------入生产库
  insert into Rm_Project@smt
    (id,
     spec_id,
     REGION_ID,
     Project_Status_Id,
     --COSTCENTERCODE,
     CONTACT_CONSTRUCTION_UNIT,
     CONSTRUCTION_UNIT,
     NAME,
     DESIGNER,
     CODE,
     solid_state_engineering,
     sharding_id)
    select ID,
           2930010001,
           decode(a.sharding_id,
                  131000000000000001216375,
                  (case
                    when companyname like '%石家庄%' then
                     131000000000000001216376
                    when companyname like '%邯郸%' then
                     131000000000000001216383
                    when companyname like '%沧州%' then
                     131000000000000001216375
                    when companyname like '%邢台%' then
                     131000000000000001216412
                    when companyname like '%衡水%' then
                     131000000000000001216397
                    when companyname like '%保定%' then
                     131000000000000001216377
                    when companyname like '%雄安%' then
                     131000000000000001216377
                    when companyname like '%张家口%' then
                     131000000000000001216396
                    when companyname like '%承德%' then
                     131000000000000001216413
                    when companyname like '%唐山%' then
                     131000000000000001216380
                    when companyname like '%廊坊%' then
                     131000000000000001216378
                    when companyname like '%秦皇岛%' then
                     131000000000000001216379
                     else
                     131000000000000001216376
                  end),
                  a.sharding_id) regionid,
           (select decode(n.nm_dict_value, 0, 80204667, 3, 80204668)
              from t_itsp_nm_dict_match n
             where n.itsp_dict_value = a.PROJECTSTATE
               and n.nm_tab_name = 'PROJECT'
               and n.nm_colu_name = 'STATE'),
           CONSTRUCTOR,
           NVL(COMPANYNAME, '未录入'),
           NVL(NAME, '未录入'),
           DESIGNER,
           NVL(NO, '未录入'),
           (select nvl(chprojstatus, '未知')
                    from de_om_v_o_cprojstatus
                   where mdata_id = a.projectstate),
           decode(a.sharding_id,
                  131000000000000001216375,
                  (case
                    when companyname like '%石家庄%' then
                     311
                    when companyname like '%邯郸%' then
                     310
                    when companyname like '%沧州%' then
                     317
                    when companyname like '%邢台%' then
                     319
                    when companyname like '%衡水%' then
                     318
                    when companyname like '%保定%' then
                     312
                    when companyname like '%雄安%' then
                     312
                    when companyname like '%张家口%' then
                     313
                    when companyname like '%承德%' then
                     314
                    when companyname like '%唐山%' then
                     315
                    when companyname like '%廊坊%' then
                     316
                    when companyname like '%秦皇岛%' then
                     335
                     else
                     311
                  end),
                  131000000000000001216376,
                  311,
                  131000000000000001216377,
                  312,
                  131000000000000001216396,
                  313,
                  131000000000000001216413,
                  314,
                  131000000000000001216378,
                  316,
                  131000000000000001216379,
                  335,
                  131000000000000001216380,
                  315,
                  131000000000000001216383,
                  310,
                  131000000000000001216397,
                  318,
                  131000000000000001216412,
                  319,
                  13)
      from Rm_Project_oip a
     where a.is_mv = 0
       and a.operation = opr;

  insert into Re_Project_Project_Ccs@smt
    (Project_Id,
     Is_Partner_Id,
     Sanma_Type_Id,
     Plan_Type,
     Special_Small_Type)
    select ID, 80201002, 90402827, plantype, specialsmalltype
      from Rm_Project_oip a
     where a.is_mv = 0
       and a.operation = opr;
  commit;
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (ENDDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  commit;
  ---- log结束
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || ' _ ' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || ' 存过报错(批量新增异常) ：有脏数据, 开始单条循环遍历 '
     WHERE ID = V_SEQ;
    commit;
    ------游标单独新增(或者删除)
    P_OIP_RM_PROJECT_I_xz(opr);
    ---异常记录结束
end P_OIP_RM_PROJECT_I_PL_XZ;
/

prompt
prompt Creating procedure P_OIP_RR_ASSET_D_XZ
prompt ======================================
prompt
create or replace procedure P_OIP_RR_ASSET_D_XZ(opr number) is
  -----三码资产物料映射表删除
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RR_ASSET_D',
     '三码资产卡实删除',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '三码资产卡实删除 P_OIP_RR_ASSET_D';
  ------头log结尾
  delete from ITSP.RR_ASSET_ENTITY a
   where exists (select 1
            from RR_ASSET_ENTITY_OIP b
           where a.id = b.id
             and b.operation = opr
             and b.is_mv = 0);
  delete from RR_ASSET_ENTITY@SMT a
   where exists (select 1
            from RR_ASSET_ENTITY_OIP b
           where a.id = b.id
             and b.operation = opr
             and b.is_mv = 0);
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (ENDDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  ---- log结束
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错'
     WHERE ID = V_SEQ;
    ---异常记录结束
end P_OIP_RR_ASSET_D_XZ;
/

prompt
prompt Creating procedure P_OIP_RR_ASSET_I_XZ
prompt ======================================
prompt
create or replace procedure P_OIP_RR_ASSET_I_XZ(opr1 number) is
  ---------------------进行单条遍历循环
begin
  ---插入工程
  for R in (select a.id,
                   a.spec_id,
                   a.operation,
                   a.entity_id,
                   a.entity_spec_id,
                   a.asset_id
              from RR_ASSET_ENTITY_OIP a
             where a.operation = opr1
               and a.is_mv = 0) loop
    begin
      ----只是新增
      if opr1 = 0 then
        insert into ITSP.RR_ASSET_ENTITY
          (id,
           code,
           name,
           spec_id,
           asset_id,
           entity_id,
           entity_spec_id,
           notes,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp,
           asset_spec_id)
          select id,
                 code,
                 name,
                 spec_id,
                 asset_id,
                 entity_id,
                 entity_spec_id,
                 notes,
                 creator_id,
                 create_date,
                 modifier_id,
                 modify_date,
                 version,
                 sharding_id,
                 time_stamp,
                 asset_spec_id
            from RR_ASSET_ENTITY_OIP a
           where a.id = R.Id;
        ----入统生产库
        insert into RR_ASSET_ENTITY@smt
          (id,
           code,
           name,
           spec_id,
           asset_id,
           entity_id,
           entity_spec_id,
           notes,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp,
           asset_spec_id)
          select id,
                 code,
                 name,
                 spec_id,
                 asset_id,
                 entity_id,
                 entity_spec_id,
                 notes,
                 nvl(creator_id, 1),
                 nvl(create_date, sysdate),
                 nvl(modifier_id, 1),
                 nvl(modify_date, sysdate),
                 nvl(version, 1),
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) sharding_id,
                 nvl(time_stamp, SYSTIMESTAMP),
                 asset_spec_id
            from RR_ASSET_ENTITY_OIP a
           where a.id = R.Id;
      elsif opr1 = 1 then
        -------先删除再新增
        delete from ITSP.RR_ASSET_ENTITY a where a.id = R.Id;
        delete from RR_ASSET_ENTITY@SMT a where a.id = R.Id;
        ---------------新增统一库
        insert into ITSP.RR_ASSET_ENTITY
          (id,
           code,
           name,
           spec_id,
           asset_id,
           entity_id,
           entity_spec_id,
           notes,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp,
           asset_spec_id)
          select id,
                 code,
                 name,
                 spec_id,
                 asset_id,
                 entity_id,
                 entity_spec_id,
                 notes,
                 creator_id,
                 create_date,
                 modifier_id,
                 modify_date,
                 version,
                 sharding_id,
                 time_stamp,
                 asset_spec_id
            from RR_ASSET_ENTITY_OIP a
           where a.id = R.Id;
        ----入统生产库
        insert into RR_ASSET_ENTITY@smt
          (id,
           code,
           name,
           spec_id,
           asset_id,
           entity_id,
           entity_spec_id,
           notes,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp,
           asset_spec_id)
          select id,
                 code,
                 name,
                 spec_id,
                 asset_id,
                 entity_id,
                 entity_spec_id,
                 notes,
                 nvl(creator_id, 1),
                 nvl(create_date, sysdate),
                 nvl(modifier_id, 1),
                 nvl(modify_date, sysdate),
                 nvl(version, 1),
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) sharding_id,
                 nvl(time_stamp, SYSTIMESTAMP),
                 asset_spec_id
            from RR_ASSET_ENTITY_OIP a
           where a.id = R.Id;
      elsif opr1 = 2 then
        delete from ITSP.RR_ASSET_ENTITY a where a.id = R.Id;
        delete from RR_ASSET_ENTITY@SMT a where a.id = R.Id;
      end if;
      commit;
      ---异常
    EXCEPTION
      WHEN OTHERS THEN
        rollback;
        insert into OIP_ERROR_DETAIL
          (table_en, id, operation, create_date)
          select 'RR_ASSET_ENTITY', R.Id, R.Operation, sysdate from dual;
        commit;
    end;
  end loop;
end P_OIP_RR_ASSET_I_XZ;
/

prompt
prompt Creating procedure P_OIP_RR_ASSET_I_PL_XZ
prompt =========================================
prompt
create or replace procedure P_OIP_RR_ASSET_I_PL_XZ(opr number) is
  V_NOWOBJECT VARCHAR2(255);
  V_SEQ       NUMBER;
  V_ERRORMSG  VARCHAR2(255);
begin
  -----------------------日子记录----------
  ---log头记录
  SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
  INSERT INTO RUN_OIP_LOG
    (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
  VALUES
    (V_SEQ,
     'P_OIP_RR_ASSET_I_PL',
     '三码资产卡实',
     '',
     '',
     SYSDATE,
     NULL,
     '');
  COMMIT;
  v_nowobject := '三码资产卡实 P_OIP_RR_ASSET_I_PL';
  ------头log结尾
  ----入统一库
  insert into ITSP.RR_ASSET_ENTITY
    (id,
     code,
     name,
     spec_id,
     asset_id,
     entity_id,
     entity_spec_id,
     notes,
     creator_id,
     create_date,
     modifier_id,
     modify_date,
     version,
     sharding_id,
     time_stamp,
     asset_spec_id)
    select id,
           code,
           name,
           spec_id,
           asset_id,
           entity_id,
           entity_spec_id,
           notes,
           creator_id,
           create_date,
           modifier_id,
           modify_date,
           version,
           sharding_id,
           time_stamp,
           asset_spec_id
      from RR_ASSET_ENTITY_OIP a
     where operation = opr
       and is_mv = 0;
  ----入统生产库
  insert into RR_ASSET_ENTITY@smt
    (id,
     code,
     name,
     spec_id,
     asset_id,
     entity_id,
     entity_spec_id,
     notes,
     creator_id,
     create_date,
     modifier_id,
     modify_date,
     version,
     sharding_id,
     time_stamp,
     asset_spec_id)
    select id,
           code,
           name,
           spec_id,
           asset_id,
           entity_id,
           entity_spec_id,
           notes,
           nvl(creator_id, 1),
           nvl(create_date, sysdate),
           nvl(modifier_id, 1),
           nvl(modify_date, sysdate),
           nvl(version, 1),
           (select t.id
              from t_sharding_code t
             where t.itspid = a.SHARDING_ID) sharding_id,
           nvl(time_stamp, SYSTIMESTAMP),
           asset_spec_id
      from RR_ASSET_ENTITY_OIP a
     where operation = opr
       and is_mv = 0;
  ----log记录
  UPDATE RUN_OIP_LOG
     set USETIME = (ENDDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
   WHERE ID = V_SEQ;
  ---- log结束
  commit;
  ------------异常记录
EXCEPTION
  WHEN OTHERS THEN
    rollback;
    V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
    UPDATE RUN_OIP_LOG
       SET USETIME  = (ENDDATE - STARTDATE) * 60 * 24,
           SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                      dbms_utility.format_error_backtrace(),
           COMMENTS = v_nowobject || '存过报错(批量新增异常)：有脏数据,开始单条循环遍历'
     WHERE ID = V_SEQ;
    commit;
    ------游标单独新增(或者删除)
    P_OIP_RR_ASSET_I_XZ(opr);
end P_OIP_RR_ASSET_I_PL_XZ;
/

prompt
prompt Creating procedure P_OIP_RR_PROJECT_I_XZ
prompt ========================================
prompt
create or replace procedure P_OIP_RR_PROJECT_I_XZ(opr1 number) is
  V_ERRORMSG varchar2(255);
  ---------------------异常后进行单条遍历循环
begin
  ----工程关系建立
  for R in (select a.id,
                   a.spec_id,
                   a.operation,
                   a.entity_id,
                   a.entity_spec_id,
                   a.project_id
              from RR_PROJECT_ENTITY_OIP a
             where a.operation = opr1
               and a.is_mv = 0) loop
    begin
      ----只是新增
      if opr1 = 0 then
        insert into itsp.RR_PROJECT_ENTITY
          (ID,
           PROJECT_ID,
           MODIFIER_ID,
           SPEC_ID,
           ENTITY_SPEC_ID,
           CODE,
           ENTITY_ID,
           SHARDING_ID,
           PROJECT_SPEC_ID,
           IS_OLD,
           NAME,
           CREATE_DATE,
           VERSION,
           NOTES,
           CREATOR_ID,
           MODIFY_DATE,
           TIME_STAMP)
          select ID,
                 PROJECT_ID,
                 MODIFIER_ID,
                 SPEC_ID,
                 ENTITY_SPEC_ID,
                 CODE,
                 ENTITY_ID,
                 SHARDING_ID,
                 PROJECT_SPEC_ID,
                 IS_OLD,
                 NAME,
                 CREATE_DATE,
                 VERSION,
                 NOTES,
                 CREATOR_ID,
                 MODIFY_DATE,
                 TIME_STAMP
            from RR_PROJECT_ENTITY_OIP a
           where a.id = R.Id;
        ----------入生产库
        Insert Into RR_PROJECT_ENTITY@smt
          (Id,
           Code,
           Name,
           Spec_Id,
           Project_Id,
           Entity_Id,
           Entity_Spec_Id,
           Notes,
           Creator_Id,
           Create_Date,
           Modifier_Id,
           Modify_Date,
           Version,
           Time_Stamp,
           Sharding_Id,
           Project_Spec_Id)
          Select ID,
                 Code,
                 Name,
                 80110201430209310000,
                 project_id,
                 Entity_Id,
                 801102010100001,
                 Notes,
                 nvl(Creator_Id, 1),
                 Nvl(Create_Date, Sysdate),
                 nvl(Modifier_Id, 1),
                 Nvl(Modify_Date, Sysdate),
                 Nvl(Version, 1),
                 nvl(Time_Stamp, SYSTIMESTAMP),
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) SHARDING_ID,
                 2930010001
            from RR_PROJECT_ENTITY_OIP a
           where a.id = R.Id;
      elsif opr1 = 1 then
        -------先删除再新增
        delete from itsp.RR_PROJECT_ENTITY a where a.id = R.Id;
        delete from RR_PROJECT_ENTITY@smt a where a.id = R.Id;
        ---------------新增统一库
        insert into itsp.RR_PROJECT_ENTITY
          (ID,
           PROJECT_ID,
           MODIFIER_ID,
           SPEC_ID,
           ENTITY_SPEC_ID,
           CODE,
           ENTITY_ID,
           SHARDING_ID,
           PROJECT_SPEC_ID,
           IS_OLD,
           NAME,
           CREATE_DATE,
           VERSION,
           NOTES,
           CREATOR_ID,
           MODIFY_DATE,
           TIME_STAMP)
          select ID,
                 PROJECT_ID,
                 MODIFIER_ID,
                 SPEC_ID,
                 ENTITY_SPEC_ID,
                 CODE,
                 ENTITY_ID,
                 SHARDING_ID,
                 PROJECT_SPEC_ID,
                 IS_OLD,
                 NAME,
                 CREATE_DATE,
                 VERSION,
                 NOTES,
                 CREATOR_ID,
                 MODIFY_DATE,
                 TIME_STAMP
            from RR_PROJECT_ENTITY_OIP a
           where a.id = R.Id;
        ----------入生产库
        Insert Into RR_PROJECT_ENTITY@smt
          (Id,
           Code,
           Name,
           Spec_Id,
           Project_Id,
           Entity_Id,
           Entity_Spec_Id,
           Notes,
           Creator_Id,
           Create_Date,
           Modifier_Id,
           Modify_Date,
           Version,
           Time_Stamp,
           Sharding_Id,
           Project_Spec_Id)
          Select ID,
                 Code,
                 Name,
                 80110201430209310000,
                 project_id,
                 Entity_Id,
                 801102010100001,
                 Notes,
                 nvl(Creator_Id, 1),
                 Nvl(Create_Date, Sysdate),
                 nvl(Modifier_Id, 1),
                 Nvl(Modify_Date, Sysdate),
                 Nvl(Version, 1),
                 nvl(Time_Stamp, SYSTIMESTAMP),
                 (select t.id
                    from t_sharding_code t
                   where t.itspid = a.SHARDING_ID) SHARDING_ID,
                 2930010001
            from RR_PROJECT_ENTITY_OIP a
           where a.id = R.Id;
      elsif opr1 = 2 then
        -------先删除
        delete from itsp.RR_PROJECT_ENTITY a where a.id = R.Id;
        delete from RR_PROJECT_ENTITY@smt a where a.id = R.Id;
      end if;
      commit;
      ---异常
    EXCEPTION
      WHEN OTHERS THEN
        rollback;
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        insert into OIP_ERROR_LOG
          (table_en, id, operation, create_date, sqlerror)
          select 'RR_PROJECT_ENTITY_OIP',
                 R.Id,
                 R.Operation,
                 sysdate,
                 V_ERRORMSG || chr(10) ||
                 dbms_utility.format_error_backtrace()
            from dual;
        commit;
    end;
  end loop;
end P_OIP_RR_PROJECT_I_XZ;
/

prompt
prompt Creating package body P_ITSP_OIP_3MZY_BY_PL_XZ
prompt ==============================================
prompt
CREATE OR REPLACE PACKAGE BODY P_ITSP_OIP_3MZY_BY_PL_XZ IS
  PROCEDURE P_OIP_IN as
    --包入口
  begin
    V_NOWOBJECT := '开始执行存过';
    ---打标
    P_OIP_TAG;
    ---备份
    P_OIP_BAK;
    ---新增
    P_OIP_I;
    ---删除
    P_OIP_D;
    ---更新
    P_OIP_U;
    ---标记及其映射射删除
    P_OIP_D_TAG;
  
  end P_OIP_IN;

  -----打标程序
  PROCEDURE P_OIP_TAG as
  begin
    ---头log记录
    v_nowobject := 'START';
    V_COUNT     := 0;
    SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT INTO RUN_OIP_LOG
      (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES
      (V_SEQ, 'P_OIP_TAG', '打标程序', '', '', SYSDATE, NULL, '');
    COMMIT;
    ---头log结尾
    /*Select Max(GREATETIME) Into v_GREATETIME From RM_ASSET_OIP;*/
    for R in (select a.table_name_en from T_OIP_TABLES a) loop
      V_SQL := 'update /*+parallel(a,5)+*/ ' || R.Table_Name_En ||
               ' a set a.is_mv= 0';
      EXECUTE IMMEDIATE V_SQL;
      commit;
    end loop;
    ----log记录
    UPDATE RUN_OIP_LOG
       SET USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
     WHERE ID = V_SEQ;
    commit;
    ---- log结束
    ----异常记录
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG
         SET USETIME  = (SYSDATE - STARTDATE) * 60 * 24,
             SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                        dbms_utility.format_error_backtrace(),
             COMMENTS = '存过报错'
       WHERE ID = V_SEQ;
      COMMIT;
      ---异常记录结束
  end P_OIP_TAG;
  --------------------打标程序

  -----------------------数据备份
  PROCEDURE P_OIP_BAK as
  begin
    ---头log记录
    V_COUNT := 0;
    SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT INTO RUN_OIP_LOG
      (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES
      (V_SEQ, 'P_OIP_BAK', '备份', '', '', SYSDATE, NULL, '');
    COMMIT;
    ---头log结尾
    ----数据备份
    for R in (select a.table_name_en from T_OIP_TABLES a) loop
      V_SQL := 'insert into ' || replace(R.table_name_en, '_OIP') ||
               '_BAK
select /*+ parallel(a,5)*/
 *
  from ' || R.Table_Name_En || ' a
 where a.is_mv = 0';
      begin
        EXECUTE IMMEDIATE V_SQL;
        commit;
        ----异常记录
      EXCEPTION
        WHEN OTHERS THEN
          V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
          UPDATE RUN_OIP_LOG
             SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                            dbms_utility.format_error_backtrace(),
                 COMMENTS = COMMENTS || R.Table_Name_En
           WHERE ID = V_SEQ;
          COMMIT;
      end;
    end loop;
    ----log记录
    UPDATE RUN_OIP_LOG
       SET USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
     WHERE ID = V_SEQ;
    commit;
    ---- log结束
  
  end P_OIP_BAK;
  -------------------数据备份

  -----------------新增
  PROCEDURE P_OIP_I as
  begin
    ---头log记录
    V_COUNT := 0;
    SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT INTO RUN_OIP_LOG
      (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES
      (V_SEQ, 'P_OIP_I', '批量新增', '', '', SYSDATE, NULL, '');
    COMMIT;
    ---头log结尾
    begin
      v_nowobject := '工程信息新增 P_OIP_RM_PROJECT_I_PL'; --新综修改
      P_OIP_RM_PROJECT_I_PL_XZ(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产卡片 P_OIP_RM_ASSET_I_PL'; --新综修改
      P_OIP_RM_ASSET_I_PL_XZ(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '物料信息 P_OIP_RM_MATERIEL_I_PL'; --新综修改
      P_OIP_RM_MATERIEL_I_PL_XZ(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产目录信息类 P_OIP_MM_ASSET_I_PL'; --无改动
      P_OIP_MM_ASSET_I_PL(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '物料目录信息类 P_OIP_MM_MATERIEL_I_PL'; --无改动
      P_OIP_MM_MATERIEL_I_PL(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '他域主数据 P_OIP_DM_OMDATA_I_PL'; --无改动
      P_OIP_DM_OMDATA_I_PL(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '三码映射表 P_OIP_MM_MAR_MAPPING_I_PL'; --无改动
      P_OIP_MM_MAR_MAPPING_I_PL(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '三码资产物料映射表 P_OIP_A_M_MAPPING_I_PL'; --无改动
      P_OIP_A_M_MAPPING_I_PL(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产与实体关系 P_OIP_RR_ASSET_I_PL'; --新综修改
      P_OIP_RR_ASSET_I_PL_XZ(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '工程与实体关系 P_OIP_RR_PROJECT_I_PL'; --新综修改
      P_OIP_RR_PROJECT_I_XZ(0);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    ----log记录
    UPDATE RUN_OIP_LOG
       set USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
     WHERE ID = V_SEQ;
    commit;
    ---- log结束
  
  end P_OIP_I;
  -------------------新增

  ------------------删除
  PROCEDURE P_OIP_D as
  begin
    ---头log记录
    V_COUNT := 0;
    SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT INTO RUN_OIP_LOG
      (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES
      (V_SEQ, 'P_OIP_D', '批量删除', '', '', SYSDATE, NULL, '');
    COMMIT;
    ---头log结尾
    begin
      v_nowobject := '工程信息删除 P_OIP_RM_PROJECT_D';
      P_OIP_RM_PROJECT_D_XZ(2); --新综修改
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产信息删除 P_OIP_RM_ASSET_D';
      P_OIP_RM_ASSET_D_XZ(2); --新综修改
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '物料信息删除 P_OIP_RM_MATERIEL_D';
      P_OIP_RM_MATERIEL_D_XZ(2); --新综修改
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产目录信息类删除 P_OIP_MM_ASSET_I_PL';
      P_OIP_MM_ASSET_D(2); --无改动
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '物料目录信息类删除 P_OIP_MM_MATERIEL_I_PL';
      P_OIP_MM_MATERIEL_D(2); --无改动
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '他域主数据删除 P_OIP_DM_OMDATA_D';
      P_OIP_DM_OMDATA_D(2); --无改动
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '三码映射表 P_OIP_MM_MAR_MAPPING_D';
      P_OIP_MM_MAR_MAPPING_D(2); --无改动
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '三码资产物料映射表 P_OIP_A_M_MAPPING_D';
      P_OIP_A_M_MAPPING_D(2); --无改动
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '工程与实体关系删除 P_OIP_RR_PROJECT_I_PL'; --新综修改
      P_OIP_RR_PROJECT_I_XZ(2);
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产与实体关系删除 P_OIP_RR_ASSET_I_PL';
      P_OIP_RR_ASSET_D_XZ(2); --新综修改
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    ----log记录
    UPDATE RUN_OIP_LOG
       set USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
     WHERE ID = V_SEQ;
    commit;
    ---- log结束
  
  end P_OIP_D;
  ------------------删除

  ------------------更新
  PROCEDURE P_OIP_U as
  begin
    ---头log记录
    V_COUNT := 0;
    SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT INTO RUN_OIP_LOG
      (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES
      (V_SEQ, 'P_OIP_U', '批量删除', '', '', SYSDATE, NULL, '');
    COMMIT;
    ---头log结尾
    begin
      v_nowobject := '工程信息更新 P_OIP_RM_PROJECT_U'; --新综修改
      P_OIP_RM_PROJECT_D_XZ(1);
      P_OIP_RM_PROJECT_I_PL_XZ(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产信息更新 P_OIP_RM_ASSET_U'; --新综修改
      P_OIP_RM_ASSET_D_XZ(1);
      P_OIP_RM_ASSET_I_PL_XZ(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '物料信息更新 P_OIP_RM_MATERIEL_U'; --新综修改
      P_OIP_RM_MATERIEL_D_XZ(1);
      P_OIP_RM_MATERIEL_I_PL_XZ(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产目录信息类 P_OIP_MM_ASSET_I_PL'; --无改动
      P_OIP_MM_ASSET_D(1);
      P_OIP_MM_ASSET_I_PL(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '物料目录信息类 P_OIP_MM_MATERIEL_I_PL'; --无改动
      P_OIP_MM_MATERIEL_D(1);
      P_OIP_MM_MATERIEL_I_PL(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '他域主数据 P_OIP_DM_OMDATA_U'; --无改动
      P_OIP_DM_OMDATA_D(1);
      P_OIP_DM_OMDATA_I_PL(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '三码映射表 P_OIP_MM_MAR_MAPPING_U'; --无改动
      P_OIP_MM_MAR_MAPPING_D(1);
      P_OIP_MM_MAR_MAPPING_I_PL(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '三码资产物料映射表 P_OIP_A_M_MAPPING_U'; --无改动
      P_OIP_A_M_MAPPING_D(1);
      P_OIP_A_M_MAPPING_I_PL(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '工程与实体关系更新 P_OIP_RR_PROJECT_I_PL'; --新综修改
      P_OIP_RR_PROJECT_I_XZ(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
    begin
      v_nowobject := '资产与实体关系更新 P_OIP_RR_ASSET_I_PL'; --新综修改
      P_OIP_RR_ASSET_D(1);
      P_OIP_RR_ASSET_I_PL(1);
    EXCEPTION
      WHEN OTHERS THEN
        V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
        UPDATE RUN_OIP_LOG
           SET SQLERROR = SQLERROR || '_' || V_ERRORMSG || chr(10) ||
                          dbms_utility.format_error_backtrace(),
               COMMENTS = v_nowobject || '存过报错'
         WHERE ID = V_SEQ;
        COMMIT;
    end;
  
    ----log记录
    UPDATE RUN_OIP_LOG
       set USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
     WHERE ID = V_SEQ;
    commit;
    ---- log结束
  
  end P_OIP_U;
  ----------更新

  -------标记及其映射射删除
  PROCEDURE P_OIP_D_TAG as
  begin
    ---头log记录
    V_COUNT := 0;
    SELECT S_RUN_OIP_LOG.NEXTVAL INTO V_SEQ FROM DUAL;
    INSERT INTO RUN_OIP_LOG
      (ID, NAME, CODE, SQLCODE, SQLERROR, STARTDATE, ENDDATE, COMMENTS)
    VALUES
      (V_SEQ,
       'P_OIP_D_TAG',
       '标记及其映射射删除',
       '',
       '',
       SYSDATE,
       NULL,
       '');
    COMMIT;
    ---头log结尾
    v_nowobject := '打标数据清理 P_OIP_D_TAG';
    for R in (select a.table_name_en from T_OIP_TABLES a) loop
      V_SQL := 'delete from ' || R.Table_Name_En || ' a where a.is_mv= 0';
      EXECUTE IMMEDIATE V_SQL;
      commit;
    end loop;
    ----log记录
    UPDATE RUN_OIP_LOG
       set USETIME = (SYSDATE - STARTDATE) * 60 * 24, ENDDATE = SYSDATE
     WHERE ID = V_SEQ;
    commit;
    ----异常记录
  EXCEPTION
    WHEN OTHERS THEN
      V_ERRORMSG := SUBSTR(SQLERRM, 1, 255);
      UPDATE RUN_OIP_LOG
         SET USETIME  = (SYSDATE - STARTDATE) * 60 * 24,
             SQLERROR = SQLCODE || '_' || V_ERRORMSG || chr(10) ||
                        dbms_utility.format_error_backtrace(),
             COMMENTS = v_nowobject || '存过报错'
       WHERE ID = V_SEQ;
      COMMIT;
  end P_OIP_D_TAG;
  -------标记及其映射射删除
END P_ITSP_OIP_3MZY_BY_PL_XZ;
/


spool off
