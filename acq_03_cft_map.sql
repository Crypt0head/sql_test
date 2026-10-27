CREATE TABLE {acq_03_cft_map}
AS
select
  cast(e.party_id as string) as cdi_id,
  cast(e.cmo_ext_party_source_id as string) as cft_id
from cdiul.ext_id_org e
where cast(e.party_id as string) in (
    select cdi_id from {acq_02_cdi_map} where cdi_id is not null
  )
  and upper(cast(e.cmo_ext_source_system as string)) like 'CFT%'
