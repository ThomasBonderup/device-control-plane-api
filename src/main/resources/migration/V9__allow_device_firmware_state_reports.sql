begin;

alter table control_plane.device_firmware_state
  alter column job_id drop not null;

alter table control_plane.device_firmware_state
  drop constraint device_firmware_state_ota_status_chk;

alter table control_plane.device_firmware_state
  add constraint device_firmware_state_ota_status_chk
  check (ota_status in (
    'CURRENT',
    'REQUESTED',
    'QUEUED',
    'DOWNLOADING',
    'INSTALLING',
    'IN_PROGRESS',
    'SUCCEEDED',
    'FAILED',
    'REJECTED',
    'CANCELED',
    'TIMED_OUT'
  ));

commit;