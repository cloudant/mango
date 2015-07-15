-module(mango_httpd_handlers).

-export([url_handler/1, db_handler/1, design_handler/1]).

url_handler(_) -> no_match.

db_handler(<<"_index">>)        -> fun mango_httpd:handle_req/2;
db_handler(<<"_explain">>)      -> fun mango_httpd:handle_req/2;
db_handler(<<"_find">>)         -> fun mango_httpd:handle_req/2;
db_handler(_) -> no_match.

design_handler(_) -> no_match.
