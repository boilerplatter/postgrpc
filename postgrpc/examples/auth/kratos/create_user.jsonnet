function(ctx) {
  statement: 'CREATE ROLE "' + ctx.identity.id + '" NOLOGIN NOSUPERUSER NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS'
}
