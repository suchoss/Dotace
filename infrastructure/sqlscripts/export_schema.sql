-- script which prepares output tables
CREATE SCHEMA export;

create table export.dotacejson(
iddotace text,
data json,
nazevzdroje text
);

CREATE UNIQUE INDEX idx_dotacejson ON export.dotacejson(iddotace, nazevzdroje);