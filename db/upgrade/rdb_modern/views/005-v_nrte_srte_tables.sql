use rdb_modern;
create view dbo.v_nrt_srte_totalidm as 
select
	unique_cd  ,
	SRT_reference as SRT_reference ,
	format as format ,
	label as label 
	from nbs_srte..totalidm;

use rdb_modern;
create view dbo.v_nrt_srte_imrdbmapping as 
select * from nbs_srte..imrdbmapping;

use rdb_modern;
create view dbo.v_nrt_srte_codeset as 
select * from nbs_srte..codeset;

use rdb_modern;
create view dbo.v_nrt_srte_code_value_general as select * from nbs_srte..Code_Value_General v;


use rdb_modern;
create view dbo.v_codeset as 
select m.unique_cd as CD ,
	RDB_table as TBL_NM ,
	RDB_attribute as COL_NM ,
	c.source_version_txt as CD_SYS_VER,
	null as DATA_TYPE,null as DATA_LENGTH,
	t.srt_reference as code_set_nm,
	t.label as cd_desc
from dbo.v_nrt_srte_imrdbmapping m
left join dbo.v_nrt_srte_totalidm t on t.unique_cd = m.unique_cd
left join dbo.v_nrt_srte_codeset c on c.code_set_nm = t.srt_reference
;


use rdb_modern;
create view dbo.v_code_value_general as 
  select  v.code as CODE_VAL, 
         v.code_short_desc_txt as CODE_DESC, 
         v.code_system_cd as CODE_SYS_CD, 
         v.code_system_desc_txt as CODE_SYS_CD_DESC,
         v.effective_from_time as CODE_EFF_DT, 
		 v.effective_to_time as CODE_END_DT
		 ,c.cd
   from dbo.v_codeset c join 
   dbo.v_nrt_srte_code_Value_General v
   on c.code_set_nm = v.code_set_nm 
  --- order by v.code;
   ;
  
  select * from rdb..code_val_general order by code_key asc;
