create table report_input
(
    user_id bigint,
    ts      timestamp,
    context json,
    ip      varchar
);

create table data_error
(
    api_report varchar,
    api_date   timestamp,
    row_text   varchar,
    error_text varchar,
    ins_ts     timestamp
);