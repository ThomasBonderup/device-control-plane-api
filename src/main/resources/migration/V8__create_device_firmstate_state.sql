CREATE TABLE control_plane.device_firmware_state (
    asset_id bigint PRIMARY KEY REFERENCES public.assets(id) ON DELETE CASCADE,

    device_id text NOT NULL,
    job_id text NOT NULL,
    ota_status varchar(20) NOT NULL,
    firmware_version text NOT NULL,
    previous_firmware_version text,
    firmware_git_sha text,
    firmware_hash text,
    build_time timestamptz,
    hardware_model text,
    bootloader_version text,
    secure_boot_enabled boolean NOT NULL DEFAULT false,

    reported_at timestamptz NOT NULL,
    received_at_utc timestamptz NOT NULL,

    source_kind text NOT NULL,
    source_actor text,
    source_topic text,
    source_partition integer,
    source_offset bigint,

    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT device_firmware_state_source_kind_chk
    CHECK (source_kind IN (
        'device_report',
        'manual_observation',
        'ota_controller',
        'import'
    )),

    CONSTRAINT device_firmware_state_ota_status_chk
    CHECK (ota_status IN (
        'SUCCEEDED',
        'FAILED'
    ))
);

CREATE UNIQUE INDEX idx_device_firmware_state_device_id
    ON control_plane.device_firmware_state(device_id);
