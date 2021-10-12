function(ctx) {
  statement: 'GRANT SELECT, INSERT, UPDATE, DELETE ON notes TO "' + ctx.identity.id + '"'
}
