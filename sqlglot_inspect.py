import sqlglot
expr = sqlglot.parse_one("select * from a left join b on a.id = b.id")
join = expr.args['joins'][0]
print(join.args)
