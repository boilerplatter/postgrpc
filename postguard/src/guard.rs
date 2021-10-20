use super::Error;
use inflector::Inflector;
use libpgquery_sys::{
    node, AArrayExpr, AConst, AExpr, AIndices, AIndirection, Aggref, ArrayCoerceExpr, ArrayExpr,
    BoolExpr, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen, CoalesceExpr, CoerceToDomain,
    CoerceToDomainValue, CoerceViaIo, CollateClause, CollateExpr, ColumnDef, CommonTableExpr,
    ConvertRowtypeExpr, DeleteStmt, DistinctExpr, FieldSelect, FieldStore, FromExpr, FuncCall,
    FuncExpr, GroupingFunc, GroupingSet, IndexElem, InferClause, InferenceElem, InsertStmt,
    JoinExpr, List, MinMaxExpr, MultiAssignRef, NamedArgExpr, NextValueExpr, Node, NullIfExpr,
    NullTest, ObjectWithArgs, OnConflictClause, OnConflictExpr, OpExpr, Param, RangeFunction,
    RangeSubselect, RawStmt, RelabelType, ResTarget, RowCompareExpr, RowExpr, ScalarArrayOpExpr,
    SelectStmt, SetToDefault, SortBy, SqlValueFunction, SubLink, TypeCast, TypeName, UpdateStmt,
    Var, WindowClause, WindowDef, WindowFunc, WithClause, XmlExpr,
};
use std::{
    fmt::{self, Formatter},
    str::FromStr,
};

/// Possible Commands to filter statements by. These commands are intentionally restricted to basic
/// CRUD operations only.
#[allow(missing_docs)]
#[derive(Debug, PartialEq)]
pub enum Command {
    Select,
    Update,
    Insert,
    Delete,
}

impl fmt::Display for Command {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let output = match self {
            Self::Select => "SELECT",
            Self::Update => "UPDATE",
            Self::Insert => "INSERT",
            Self::Delete => "DELETE",
        };

        formatter.write_str(output)
    }
}

/// Configurations for Allowed Statements
#[derive(Debug)]
pub enum AllowedStatements {
    /// No restrictions on statements, equivalent to `*` in a CORS-like configuration. This setting
    /// means that statement filtering of any kind is disabled, so use only in trusted
    /// environments.
    All,
    /// Set of allowed commands. An empty set means that all statements are disallowed.
    List(Vec<Command>),
}

impl AllowedStatements {
    fn contains(&self, command: &Command) -> bool {
        match self {
            Self::All => true,
            Self::List(statements) => statements.contains(command),
        }
    }
}

impl FromStr for AllowedStatements {
    type Err = Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "*" => Ok(Self::All),
            source => {
                let mut statements = vec![];

                for statement in source.to_lowercase().split(',') {
                    match statement.trim() {
                        "select" => statements.push(Command::Select),
                        "update" => statements.push(Command::Update),
                        "insert" => statements.push(Command::Insert),
                        "delete" => statements.push(Command::Delete),
                        statement => return Err(Error::StatementNotAllowed(statement.to_string())),
                    }
                }

                statements.dedup();

                Ok(Self::List(statements))
            }
        }
    }
}

impl Default for AllowedStatements {
    //allow all statements by default
    fn default() -> Self {
        Self::All
    }
}

/// Configurations for Allowed Functions (by name)
#[derive(Debug)]
pub enum AllowedFunctions {
    /// No restrictions on function execution, equivalent to `*` in a CORS-like configuration. This setting
    /// means that function filtering of any kind is disabled, so use only in trusted
    /// environments.
    All,
    /// Set of allowed functions by name. An empty set means that function exeuction of any kind is
    /// disallowed.
    List(Vec<String>),
}

impl FromStr for AllowedFunctions {
    type Err = Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "*" => Ok(Self::All),
            source => {
                let mut functions = vec![];

                for function in source.to_lowercase().split(',') {
                    let function = function.trim().to_string();

                    if function.is_snake_case() {
                        functions.push(function)
                    } else {
                        return Err(Error::InvalidFunctionName(function));
                    }
                }

                Ok(Self::List(functions))
            }
        }
    }
}

impl Default for AllowedFunctions {
    // allow all functions by default
    fn default() -> Self {
        Self::All
    }
}

/// CORS-like statement analyzer for guarding queryable methods
#[derive(Default)]
pub struct Guard {
    allowed_statements: AllowedStatements,
    allowed_functions: AllowedFunctions,
}

impl Guard {
    /// Create a new Guard from CORS-like ALLOWED-* configurations
    pub fn new(allowed_statements: AllowedStatements, allowed_functions: AllowedFunctions) -> Self {
        Self {
            allowed_statements,
            allowed_functions,
        }
    }

    /// Test a statement against the configuration
    pub fn guard(&self, statement: &str) -> Result<(), Error> {
        match (&self.allowed_statements, &self.allowed_functions) {
            // handle the most-permissive case first
            (AllowedStatements::All, AllowedFunctions::All) => Ok(()),
            // all other cases need statement parsing
            _ => {
                let statements = libpgquery_sys::parse(statement)?
                    .stmts
                    .into_iter()
                    .filter_map(|RawStmt { stmt, .. }| stmt);

                self.test(statements)
            }
        }
    }

    fn test_select(&self, statement: &SelectStmt) -> Result<(), Error> {
        let SelectStmt {
            distinct_clause,
            target_list,
            from_clause,
            where_clause,
            group_clause,
            having_clause,
            window_clause,
            values_lists,
            limit_offset,
            limit_count,
            locking_clause,
            with_clause,
            larg,
            rarg,
            ..
        } = statement;

        self.test(distinct_clause)?;
        self.test(target_list)?;
        self.test(from_clause)?;
        self.test(where_clause.as_deref())?;
        self.test(group_clause)?;
        self.test(having_clause.as_deref())?;
        self.test(window_clause)?;
        self.test(values_lists)?;
        self.test(limit_offset.as_deref())?;
        self.test(limit_count.as_deref())?;
        self.test(locking_clause)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        // test each side of UNION opts as select statements only
        if let Some(statement) = larg {
            self.test_select(statement.as_ref())?;
        }

        if let Some(statement) = rarg {
            self.test_select(statement.as_ref())?;
        }

        Ok(())
    }

    fn test_update(&self, statement: &UpdateStmt) -> Result<(), Error> {
        let UpdateStmt {
            target_list,
            where_clause,
            from_clause,
            returning_list,
            with_clause,
            ..
        } = statement;

        self.test(target_list)?;
        self.test(where_clause.as_deref())?;
        self.test(from_clause)?;
        self.test(returning_list)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        Ok(())
    }

    fn test_insert(&self, statement: &InsertStmt) -> Result<(), Error> {
        let InsertStmt {
            cols,
            select_stmt,
            on_conflict_clause,
            returning_list,
            with_clause,
            ..
        } = statement;

        self.test(cols)?;
        self.test(select_stmt.as_deref())?;
        self.test(returning_list)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        if let Some(on_conflict_clause) = on_conflict_clause {
            let OnConflictClause {
                infer,
                target_list,
                where_clause,
                ..
            } = on_conflict_clause.as_ref();

            self.test(target_list)?;
            self.test(where_clause.as_deref())?;

            if let Some(infer_clause) = infer {
                let InferClause {
                    index_elems,
                    where_clause,
                    ..
                } = infer_clause.as_ref();

                self.test(index_elems)?;
                self.test(where_clause.as_deref())?;
            }
        }

        Ok(())
    }

    fn test_delete(&self, statement: &DeleteStmt) -> Result<(), Error> {
        let DeleteStmt {
            using_clause,
            where_clause,
            returning_list,
            with_clause,
            ..
        } = statement;

        self.test(using_clause)?;
        self.test(where_clause.as_deref())?;
        self.test(returning_list)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        Ok(())
    }

    fn test_function_call(&self, func_call: &FuncCall) -> Result<(), Error> {
        let FuncCall {
            funcname,
            args,
            agg_order,
            agg_filter,
            over,
            ..
        } = func_call;

        self.test(funcname)?;
        self.test(args)?;
        self.test(agg_order)?;
        self.test(agg_filter.as_deref())?;

        if let Some(over) = over {
            self.test_window_def(over)?;
        }

        Ok(())
    }

    fn test_a_const(&self, a_const: &AConst) -> Result<(), Error> {
        let AConst { val, .. } = a_const;

        self.test(val.as_deref())
    }

    fn test_a_indices(&self, a_indices: &AIndices) -> Result<(), Error> {
        let AIndices { lidx, uidx, .. } = a_indices;

        self.test(lidx.as_deref())?;
        self.test(uidx.as_deref())?;

        Ok(())
    }

    fn test_a_indirection(&self, a_indirection: &AIndirection) -> Result<(), Error> {
        let AIndirection { arg, indirection } = a_indirection;

        self.test(arg.as_deref())?;
        self.test(indirection)?;

        Ok(())
    }

    fn test_a_expr(&self, a_expr: &AExpr) -> Result<(), Error> {
        let AExpr {
            name, lexpr, rexpr, ..
        } = a_expr;

        self.test(name)?;
        self.test(lexpr.as_deref())?;
        self.test(rexpr.as_deref())?;

        Ok(())
    }

    fn test_a_array_expr(&self, a_array_expr: &AArrayExpr) -> Result<(), Error> {
        let AArrayExpr { elements, .. } = a_array_expr;

        self.test(elements)
    }

    fn test_array_expr(&self, array_expr: &ArrayExpr) -> Result<(), Error> {
        let ArrayExpr { xpr, elements, .. } = array_expr;

        self.test(xpr.as_deref())?;
        self.test(elements)?;

        Ok(())
    }

    fn test_cte(&self, cte: &CommonTableExpr) -> Result<(), Error> {
        let CommonTableExpr {
            aliascolnames,
            ctequery,
            ctecolnames,
            ctecoltypes,
            ctecoltypmods,
            ctecolcollations,
            ..
        } = cte;

        self.test(aliascolnames)?;
        self.test(ctequery.as_deref())?;
        self.test(ctecolnames)?;
        self.test(ctecoltypes)?;
        self.test(ctecoltypmods)?;
        self.test(ctecolcollations)?;

        Ok(())
    }

    fn test_res_target(&self, target: &ResTarget) -> Result<(), Error> {
        let ResTarget {
            indirection, val, ..
        } = target;

        self.test(indirection)?;
        self.test(val.as_deref())?;

        Ok(())
    }

    fn test_list(&self, list: &List) -> Result<(), Error> {
        let List { items } = list;

        self.test(items)
    }

    fn test_case_when(&self, case_when: &CaseWhen) -> Result<(), Error> {
        let CaseWhen { expr, result, .. } = case_when;

        self.test(expr.as_deref())?;
        self.test(result.as_deref())?;

        Ok(())
    }

    fn test_case_expr(&self, case_expr: &CaseExpr) -> Result<(), Error> {
        let CaseExpr {
            arg,
            args,
            defresult,
            ..
        } = case_expr;

        self.test(arg.as_deref())?;
        self.test(args)?;
        self.test(defresult.as_deref())?;

        Ok(())
    }

    fn test_join_expr(&self, join_expr: &JoinExpr) -> Result<(), Error> {
        let JoinExpr {
            larg,
            rarg,
            using_clause,
            quals,
            alias,
            ..
        } = join_expr;

        self.test(larg.as_deref())?;
        self.test(rarg.as_deref())?;
        self.test(using_clause)?;
        self.test(quals.as_deref())?;

        if let Some(alias) = alias {
            self.test(&alias.colnames)?;
        }

        Ok(())
    }

    fn test_var(&self, var: &Var) -> Result<(), Error> {
        let Var { xpr, .. } = var;

        self.test(xpr.as_deref())
    }

    fn test_param(&self, param: &Param) -> Result<(), Error> {
        let Param { xpr, .. } = param;

        self.test(xpr.as_deref())
    }

    fn test_aggref(&self, aggref: &Aggref) -> Result<(), Error> {
        let Aggref {
            xpr,
            aggargtypes,
            aggdirectargs,
            args,
            aggorder,
            aggdistinct,
            aggfilter,
            ..
        } = aggref;

        self.test(xpr.as_deref())?;
        self.test(aggargtypes)?;
        self.test(aggdirectargs)?;
        self.test(args)?;
        self.test(aggorder)?;
        self.test(aggdistinct)?;
        self.test(aggfilter.as_deref())?;

        Ok(())
    }

    fn test_groupingfunc(&self, grouping_func: &GroupingFunc) -> Result<(), Error> {
        let GroupingFunc {
            xpr,
            args,
            refs,
            cols,
            ..
        } = grouping_func;

        self.test(xpr.as_deref())?;
        self.test(args)?;
        self.test(refs)?;
        self.test(cols)?;

        Ok(())
    }

    fn test_window_clause(&self, window_clause: &WindowClause) -> Result<(), Error> {
        let WindowClause {
            partition_clause,
            order_clause,
            start_offset,
            end_offset,
            ..
        } = window_clause;

        self.test(partition_clause)?;
        self.test(order_clause)?;
        self.test(start_offset.as_deref())?;
        self.test(end_offset.as_deref())?;

        Ok(())
    }

    fn test_window_def(&self, window_def: &WindowDef) -> Result<(), Error> {
        let WindowDef {
            partition_clause,
            order_clause,
            start_offset,
            end_offset,
            ..
        } = window_def;

        self.test(partition_clause)?;
        self.test(order_clause)?;
        self.test(start_offset.as_deref())?;
        self.test(end_offset.as_deref())?;

        Ok(())
    }

    fn test_windowfunc(&self, window_func: &WindowFunc) -> Result<(), Error> {
        let WindowFunc {
            xpr,
            args,
            aggfilter,
            ..
        } = window_func;

        self.test(xpr.as_deref())?;
        self.test(args)?;
        self.test(aggfilter.as_deref())?;

        Ok(())
    }

    fn test_func_expr(&self, func_expr: &FuncExpr) -> Result<(), Error> {
        let FuncExpr { xpr, args, .. } = func_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_named_arg_expr(&self, named_arg_expr: &NamedArgExpr) -> Result<(), Error> {
        let NamedArgExpr { xpr, arg, .. } = named_arg_expr;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_op_expr(&self, op_expr: &OpExpr) -> Result<(), Error> {
        let OpExpr { xpr, args, .. } = op_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_distinct_expr(&self, distinct_expr: &DistinctExpr) -> Result<(), Error> {
        let DistinctExpr { xpr, args, .. } = distinct_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_nullif_expr(&self, nullif_expr: &NullIfExpr) -> Result<(), Error> {
        let NullIfExpr { xpr, args, .. } = nullif_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_scalar_array_op_expr(
        &self,
        scalar_array_op_expr: &ScalarArrayOpExpr,
    ) -> Result<(), Error> {
        let ScalarArrayOpExpr { xpr, args, .. } = scalar_array_op_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_bool_expr(&self, bool_expr: &BoolExpr) -> Result<(), Error> {
        let BoolExpr { xpr, args, .. } = bool_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_sublink(&self, sublink: &SubLink) -> Result<(), Error> {
        let SubLink {
            xpr,
            testexpr,
            oper_name,
            subselect,
            ..
        } = sublink;

        self.test(xpr.as_deref())?;
        self.test(testexpr.as_deref())?;
        self.test(oper_name)?;
        self.test(subselect.as_deref())?;

        Ok(())
    }

    fn test_field_select(&self, field_select: &FieldSelect) -> Result<(), Error> {
        let FieldSelect { xpr, arg, .. } = field_select;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_field_store(&self, field_store: &FieldStore) -> Result<(), Error> {
        let FieldStore {
            xpr,
            arg,
            newvals,
            fieldnums,
            ..
        } = field_store;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;
        self.test(newvals)?;
        self.test(fieldnums)?;

        Ok(())
    }

    fn test_relabel_type(&self, relabel_type: &RelabelType) -> Result<(), Error> {
        let RelabelType { xpr, arg, .. } = relabel_type;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_coerce_via_io(&self, coerce_via_io: &CoerceViaIo) -> Result<(), Error> {
        let CoerceViaIo { xpr, arg, .. } = coerce_via_io;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_array_coerce_expr(&self, array_coerce_expr: &ArrayCoerceExpr) -> Result<(), Error> {
        let ArrayCoerceExpr { xpr, arg, .. } = array_coerce_expr;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_convert_row_type_expr(
        &self,
        convert_row_type_expr: &ConvertRowtypeExpr,
    ) -> Result<(), Error> {
        let ConvertRowtypeExpr { xpr, arg, .. } = convert_row_type_expr;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_collate_expr(&self, collate_expr: &CollateExpr) -> Result<(), Error> {
        let CollateExpr { xpr, arg, .. } = collate_expr;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_collate_clause(&self, collate_clause: &CollateClause) -> Result<(), Error> {
        let CollateClause { arg, collname, .. } = collate_clause;

        self.test(arg.as_deref())?;
        self.test(collname)?;

        Ok(())
    }

    fn test_case_test_expr(&self, case_test_expr: &CaseTestExpr) -> Result<(), Error> {
        let CaseTestExpr { xpr, .. } = case_test_expr;

        self.test(xpr.as_deref())
    }

    fn test_row_expr(&self, row_expr: &RowExpr) -> Result<(), Error> {
        let RowExpr {
            xpr,
            args,
            colnames,
            ..
        } = row_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;
        self.test(colnames)?;

        Ok(())
    }

    fn test_row_compare_expr(&self, row_compare_expr: &RowCompareExpr) -> Result<(), Error> {
        let RowCompareExpr {
            xpr,
            opnos,
            opfamilies,
            inputcollids,
            largs,
            rargs,
            ..
        } = row_compare_expr;

        self.test(xpr.as_deref())?;
        self.test(opnos)?;
        self.test(opfamilies)?;
        self.test(inputcollids)?;
        self.test(largs)?;
        self.test(rargs)?;

        Ok(())
    }

    fn test_coalesce_expr(&self, coalesce_expr: &CoalesceExpr) -> Result<(), Error> {
        let CoalesceExpr { xpr, args, .. } = coalesce_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_min_max_expr(&self, min_max_expr: &MinMaxExpr) -> Result<(), Error> {
        let MinMaxExpr { xpr, args, .. } = min_max_expr;

        self.test(xpr.as_deref())?;
        self.test(args)?;

        Ok(())
    }

    fn test_sql_value_function(&self, sql_value_function: &SqlValueFunction) -> Result<(), Error> {
        let SqlValueFunction { xpr, .. } = sql_value_function;

        self.test(xpr.as_deref())
    }

    fn test_xml_expr(&self, xml_expr: &XmlExpr) -> Result<(), Error> {
        let XmlExpr {
            xpr,
            named_args,
            args,
            ..
        } = xml_expr;

        self.test(xpr.as_deref())?;
        self.test(named_args)?;
        self.test(args)?;

        Ok(())
    }

    fn test_null_test(&self, null_test: &NullTest) -> Result<(), Error> {
        let NullTest { xpr, arg, .. } = null_test;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_boolean_test(&self, boolean_test: &BooleanTest) -> Result<(), Error> {
        let BooleanTest { xpr, arg, .. } = boolean_test;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_coerce_to_domain(&self, coerce_to_domain: &CoerceToDomain) -> Result<(), Error> {
        let CoerceToDomain { xpr, arg, .. } = coerce_to_domain;

        self.test(xpr.as_deref())?;
        self.test(arg.as_deref())?;

        Ok(())
    }

    fn test_coerce_to_domain_value(
        &self,
        coerce_to_domain_value: &CoerceToDomainValue,
    ) -> Result<(), Error> {
        let CoerceToDomainValue { xpr, .. } = coerce_to_domain_value;

        self.test(xpr.as_deref())
    }

    fn test_set_to_default(&self, set_to_default: &SetToDefault) -> Result<(), Error> {
        let SetToDefault { xpr, .. } = set_to_default;

        self.test(xpr.as_deref())
    }

    fn test_next_value_expr(&self, next_value_expr: &NextValueExpr) -> Result<(), Error> {
        let NextValueExpr { xpr, .. } = next_value_expr;

        self.test(xpr.as_deref())
    }

    fn test_inference_elem(&self, inference_elem: &InferenceElem) -> Result<(), Error> {
        let InferenceElem { xpr, expr, .. } = inference_elem;

        self.test(xpr.as_deref())?;
        self.test(expr.as_deref())?;

        Ok(())
    }

    fn test_from_expr(&self, from_expr: &FromExpr) -> Result<(), Error> {
        let FromExpr { fromlist, quals } = from_expr;

        self.test(fromlist)?;
        self.test(quals.as_deref())?;

        Ok(())
    }

    fn test_on_conflict_expr(&self, on_conflict_expr: &OnConflictExpr) -> Result<(), Error> {
        let OnConflictExpr {
            arbiter_elems,
            arbiter_where,
            on_conflict_set,
            on_conflict_where,
            excl_rel_tlist,
            ..
        } = on_conflict_expr;

        self.test(arbiter_elems)?;
        self.test(arbiter_where.as_deref())?;
        self.test(on_conflict_set)?;
        self.test(on_conflict_where.as_deref())?;
        self.test(excl_rel_tlist)?;

        Ok(())
    }

    fn test_on_conflict_clause(&self, on_conflict_clause: &OnConflictClause) -> Result<(), Error> {
        let OnConflictClause {
            infer,
            target_list,
            where_clause,
            ..
        } = on_conflict_clause;

        if let Some(infer_clause) = infer {
            self.test_infer_clause(infer_clause)?;
        }

        self.test(target_list)?;
        self.test(where_clause.as_deref())?;

        Ok(())
    }

    fn test_multi_assign_ref(&self, multi_assign_ref: &MultiAssignRef) -> Result<(), Error> {
        let MultiAssignRef { source, .. } = multi_assign_ref;

        self.test(source.as_deref())
    }

    fn test_type_cast(&self, type_cast: &TypeCast) -> Result<(), Error> {
        let TypeCast { arg, .. } = type_cast;

        self.test(arg.as_deref())
    }

    fn test_sort_by(&self, sort_by: &SortBy) -> Result<(), Error> {
        let SortBy { node, use_op, .. } = sort_by;

        self.test(node.as_deref())?;
        self.test(use_op)?;

        Ok(())
    }

    fn test_range_subselect(&self, range_subselect: &RangeSubselect) -> Result<(), Error> {
        let RangeSubselect { subquery, .. } = range_subselect;

        self.test(subquery.as_deref())
    }

    fn test_range_function(&self, range_function: &RangeFunction) -> Result<(), Error> {
        let RangeFunction {
            functions,
            coldeflist,
            ..
        } = range_function;

        self.test(functions)?;
        self.test(coldeflist)?;

        Ok(())
    }

    fn test_type_name(&self, type_name: &TypeName) -> Result<(), Error> {
        let TypeName {
            names,
            typmods,
            array_bounds,
            ..
        } = type_name;

        self.test(names)?;
        self.test(typmods)?;
        self.test(array_bounds)?;

        Ok(())
    }

    fn test_column_def(&self, column_def: &ColumnDef) -> Result<(), Error> {
        let ColumnDef {
            raw_default,
            cooked_default,
            coll_clause,
            constraints,
            fdwoptions,
            ..
        } = column_def;

        self.test(raw_default.as_deref())?;
        self.test(cooked_default.as_deref())?;
        self.test(constraints)?;
        self.test(fdwoptions)?;

        if let Some(collate_clause) = coll_clause {
            self.test_collate_clause(collate_clause)?;
        }

        Ok(())
    }

    fn test_index_elem(&self, index_elem: &IndexElem) -> Result<(), Error> {
        let IndexElem {
            expr,
            collation,
            opclass,
            ..
        } = index_elem;

        self.test(expr.as_deref())?;
        self.test(collation)?;
        self.test(opclass)?;

        Ok(())
    }

    fn test_grouping_set(&self, grouping_set: &GroupingSet) -> Result<(), Error> {
        let GroupingSet { content, .. } = grouping_set;

        self.test(content)
    }

    fn test_object_with_args(&self, object_with_args: &ObjectWithArgs) -> Result<(), Error> {
        let ObjectWithArgs {
            objname, objargs, ..
        } = object_with_args;

        self.test(objname)?;
        self.test(objargs)?;

        Ok(())
    }

    fn test_with_clause(&self, with_clause: &WithClause) -> Result<(), Error> {
        let WithClause { ctes, .. } = with_clause;

        self.test(ctes)
    }

    fn test_infer_clause(&self, infer_clause: &InferClause) -> Result<(), Error> {
        let InferClause {
            index_elems,
            where_clause,
            ..
        } = infer_clause;

        self.test(index_elems)?;
        self.test(where_clause.as_deref())?;

        Ok(())
    }

    fn test<I, N>(&self, nodes: I) -> Result<(), Error>
    where
        N: std::borrow::Borrow<Node>,
        I: IntoIterator<Item = N>,
    {
        // TODO: check if par_iter improves performance significantly (see: criterion)
        for node in nodes {
            if let Some(node) = &node.borrow().node {
                match node {
                    node::Node::SelectStmt(select_statement) => {
                        let command = Command::Select;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_select(select_statement)?;
                    }
                    node::Node::UpdateStmt(update_statement) => {
                        let command = Command::Update;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_update(update_statement)?;
                    }
                    node::Node::InsertStmt(insert_statement) => {
                        let command = Command::Insert;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_insert(insert_statement)?;
                    }
                    node::Node::DeleteStmt(delete_statement) => {
                        let command = Command::Delete;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_delete(delete_statement)?;
                    }
                    node::Node::AConst(a_const) => {
                        self.test_a_const(a_const)?;
                    }
                    node::Node::AIndices(a_indices) => {
                        self.test_a_indices(a_indices)?;
                    }
                    node::Node::AIndirection(a_indirection) => {
                        self.test_a_indirection(a_indirection)?;
                    }
                    node::Node::AExpr(a_expr) => {
                        self.test_a_expr(a_expr)?;
                    }
                    node::Node::AArrayExpr(a_array_expr) => {
                        self.test_a_array_expr(a_array_expr)?;
                    }
                    node::Node::ArrayExpr(array_expr) => {
                        self.test_array_expr(array_expr)?;
                    }
                    node::Node::CommonTableExpr(common_table_expression) => {
                        self.test_cte(common_table_expression)?;
                    }
                    node::Node::ResTarget(res_target) => {
                        self.test_res_target(res_target)?;
                    }
                    node::Node::List(list) => {
                        self.test_list(list)?;
                    }
                    node::Node::CaseWhen(case_when) => {
                        self.test_case_when(case_when)?;
                    }
                    node::Node::CaseExpr(case_expr) => {
                        self.test_case_expr(case_expr)?;
                    }
                    node::Node::JoinExpr(join_expr) => {
                        self.test_join_expr(join_expr)?;
                    }
                    node::Node::Var(var) => {
                        self.test_var(var)?;
                    }
                    node::Node::Param(param) => {
                        self.test_param(param)?;
                    }
                    node::Node::Aggref(aggref) => {
                        self.test_aggref(aggref)?;
                    }
                    node::Node::GroupingFunc(grouping_func) => {
                        self.test_groupingfunc(grouping_func)?;
                    }
                    node::Node::WindowClause(window_clause) => {
                        self.test_window_clause(window_clause)?;
                    }
                    node::Node::WindowDef(window_def) => {
                        self.test_window_def(window_def)?;
                    }
                    node::Node::WindowFunc(window_func) => {
                        self.test_windowfunc(window_func)?;
                    }
                    node::Node::FuncExpr(func_expr) => {
                        self.test_func_expr(func_expr)?;
                    }
                    node::Node::NamedArgExpr(named_arg_expr) => {
                        self.test_named_arg_expr(named_arg_expr)?;
                    }
                    node::Node::OpExpr(op_expr) => {
                        self.test_op_expr(op_expr)?;
                    }
                    node::Node::DistinctExpr(distinct_expr) => {
                        self.test_distinct_expr(distinct_expr)?;
                    }
                    node::Node::NullIfExpr(nullif_expr) => {
                        self.test_nullif_expr(nullif_expr)?;
                    }
                    node::Node::ScalarArrayOpExpr(scalar_array_op_expr) => {
                        self.test_scalar_array_op_expr(scalar_array_op_expr)?;
                    }
                    node::Node::BoolExpr(bool_expr) => {
                        self.test_bool_expr(bool_expr)?;
                    }
                    node::Node::SubLink(sublink) => {
                        self.test_sublink(sublink)?;
                    }
                    node::Node::FieldSelect(field_select) => {
                        self.test_field_select(field_select)?;
                    }
                    node::Node::FieldStore(field_store) => {
                        self.test_field_store(field_store)?;
                    }
                    node::Node::RelabelType(relabel_type) => {
                        self.test_relabel_type(relabel_type)?;
                    }
                    node::Node::CoerceViaIo(coerce_via_io) => {
                        self.test_coerce_via_io(coerce_via_io)?;
                    }
                    node::Node::ArrayCoerceExpr(array_coerce_expr) => {
                        self.test_array_coerce_expr(array_coerce_expr)?;
                    }
                    node::Node::ConvertRowtypeExpr(convert_row_type_expr) => {
                        self.test_convert_row_type_expr(convert_row_type_expr)?;
                    }
                    node::Node::CollateExpr(collate_expr) => {
                        self.test_collate_expr(collate_expr)?;
                    }
                    node::Node::CollateClause(collate_clause) => {
                        self.test_collate_clause(collate_clause)?;
                    }
                    node::Node::CaseTestExpr(case_test_expr) => {
                        self.test_case_test_expr(case_test_expr)?;
                    }
                    node::Node::RowExpr(row_expr) => {
                        self.test_row_expr(row_expr)?;
                    }
                    node::Node::RowCompareExpr(row_compare_expr) => {
                        self.test_row_compare_expr(row_compare_expr)?;
                    }
                    node::Node::CoalesceExpr(coalesce_expr) => {
                        self.test_coalesce_expr(coalesce_expr)?;
                    }
                    node::Node::MinMaxExpr(min_max_expr) => {
                        self.test_min_max_expr(min_max_expr)?;
                    }
                    node::Node::SqlvalueFunction(sql_value_function) => {
                        self.test_sql_value_function(sql_value_function)?;
                    }
                    node::Node::XmlExpr(xml_expr) => {
                        self.test_xml_expr(xml_expr)?;
                    }
                    node::Node::NullTest(null_test) => {
                        self.test_null_test(null_test)?;
                    }
                    node::Node::BooleanTest(boolean_test) => {
                        self.test_boolean_test(boolean_test)?;
                    }
                    node::Node::CoerceToDomain(coerce_to_domain) => {
                        self.test_coerce_to_domain(coerce_to_domain)?;
                    }
                    node::Node::CoerceToDomainValue(coerce_to_domain_value) => {
                        self.test_coerce_to_domain_value(coerce_to_domain_value)?;
                    }
                    node::Node::SetToDefault(set_to_default) => {
                        self.test_set_to_default(set_to_default)?;
                    }
                    node::Node::NextValueExpr(next_value_expr) => {
                        self.test_next_value_expr(next_value_expr)?;
                    }
                    node::Node::InferenceElem(inference_elem) => {
                        self.test_inference_elem(inference_elem)?;
                    }
                    node::Node::FromExpr(from_expr) => {
                        self.test_from_expr(from_expr)?;
                    }
                    node::Node::OnConflictClause(on_conflict_clause) => {
                        self.test_on_conflict_clause(on_conflict_clause)?;
                    }
                    node::Node::OnConflictExpr(on_conflict_expr) => {
                        self.test_on_conflict_expr(on_conflict_expr)?;
                    }
                    node::Node::MultiAssignRef(multi_assign_ref) => {
                        self.test_multi_assign_ref(multi_assign_ref)?;
                    }
                    node::Node::TypeCast(type_cast) => {
                        self.test_type_cast(type_cast)?;
                    }
                    node::Node::SortBy(sort_by) => {
                        self.test_sort_by(sort_by)?;
                    }
                    node::Node::RangeSubselect(range_subselect) => {
                        self.test_range_subselect(range_subselect)?;
                    }
                    node::Node::RangeFunction(range_function) => {
                        self.test_range_function(range_function)?;
                    }
                    node::Node::TypeName(type_name) => {
                        self.test_type_name(type_name)?;
                    }
                    node::Node::ColumnDef(column_def) => {
                        self.test_column_def(column_def)?;
                    }
                    node::Node::IndexElem(index_elem) => {
                        self.test_index_elem(index_elem)?;
                    }
                    node::Node::GroupingSet(grouping_set) => {
                        self.test_grouping_set(grouping_set)?;
                    }
                    node::Node::ObjectWithArgs(object_with_args) => {
                        self.test_object_with_args(object_with_args)?;
                    }
                    node::Node::WithClause(with_clause) => {
                        self.test_with_clause(with_clause)?;
                    }
                    node::Node::InferClause(infer_clause) => {
                        self.test_infer_clause(infer_clause)?;
                    }
                    // reject privileged statements and commands out-of-hand
                    node::Node::AccessPriv(..)
                    | node::Node::AlterCollationStmt(..)
                    | node::Node::AlterDatabaseSetStmt(..)
                    | node::Node::AlterDatabaseStmt(..)
                    | node::Node::AlterDefaultPrivilegesStmt(..)
                    | node::Node::AlterDomainStmt(..)
                    | node::Node::AlterEnumStmt(..)
                    | node::Node::AlterEventTrigStmt(..)
                    | node::Node::AlterExtensionContentsStmt(..)
                    | node::Node::AlterExtensionStmt(..)
                    | node::Node::AlterFdwStmt(..)
                    | node::Node::AlterForeignServerStmt(..)
                    | node::Node::AlterFunctionStmt(..)
                    | node::Node::AlterObjectDependsStmt(..)
                    | node::Node::AlterObjectSchemaStmt(..)
                    | node::Node::AlterOpFamilyStmt(..)
                    | node::Node::AlterOperatorStmt(..)
                    | node::Node::AlterOwnerStmt(..)
                    | node::Node::AlternativeSubPlan(..)
                    | node::Node::AlterPolicyStmt(..)
                    | node::Node::AlterPublicationStmt(..)
                    | node::Node::AlterRoleSetStmt(..)
                    | node::Node::AlterRoleStmt(..)
                    | node::Node::AlterSeqStmt(..)
                    | node::Node::AlterStatsStmt(..)
                    | node::Node::AlterSubscriptionStmt(..)
                    | node::Node::AlterSystemStmt(..)
                    | node::Node::AlterTableCmd(..)
                    | node::Node::AlterTableMoveAllStmt(..)
                    | node::Node::AlterTableSpaceOptionsStmt(..)
                    | node::Node::AlterTableStmt(..)
                    | node::Node::AlterTsconfigurationStmt(..)
                    | node::Node::AlterTsdictionaryStmt(..)
                    | node::Node::AlterTypeStmt(..)
                    | node::Node::AlterUserMappingStmt(..)
                    | node::Node::CallStmt(..)
                    | node::Node::CheckPointStmt(..)
                    | node::Node::ClosePortalStmt(..)
                    | node::Node::ClusterStmt(..)
                    | node::Node::CommentStmt(..)
                    | node::Node::CompositeTypeStmt(..)
                    | node::Node::ConstraintsSetStmt(..)
                    | node::Node::Constraint(..)
                    | node::Node::CopyStmt(..)
                    | node::Node::CreateAmStmt(..)
                    | node::Node::CreateCastStmt(..)
                    | node::Node::CreateConversionStmt(..)
                    | node::Node::CreateDomainStmt(..)
                    | node::Node::CreateEnumStmt(..)
                    | node::Node::CreateEventTrigStmt(..)
                    | node::Node::CreateExtensionStmt(..)
                    | node::Node::CreateFdwStmt(..)
                    | node::Node::CreateForeignServerStmt(..)
                    | node::Node::CreateForeignTableStmt(..)
                    | node::Node::CreateFunctionStmt(..)
                    | node::Node::CreateOpClassItem(..)
                    | node::Node::CreateOpClassStmt(..)
                    | node::Node::CreateOpFamilyStmt(..)
                    | node::Node::CreatePlangStmt(..)
                    | node::Node::CreatePolicyStmt(..)
                    | node::Node::CreatePublicationStmt(..)
                    | node::Node::CreateRangeStmt(..)
                    | node::Node::CreateRoleStmt(..)
                    | node::Node::CreateSchemaStmt(..)
                    | node::Node::CreateSeqStmt(..)
                    | node::Node::CreateStatsStmt(..)
                    | node::Node::CreateStmt(..)
                    | node::Node::CreateSubscriptionStmt(..)
                    | node::Node::CreateTableAsStmt(..)
                    | node::Node::CreateTableSpaceStmt(..)
                    | node::Node::CreateTransformStmt(..)
                    | node::Node::CreateTrigStmt(..)
                    | node::Node::CreateUserMappingStmt(..)
                    | node::Node::CreatedbStmt(..)
                    | node::Node::CurrentOfExpr(..)
                    | node::Node::DeallocateStmt(..)
                    | node::Node::DeclareCursorStmt(..)
                    | node::Node::DefElem(..)
                    | node::Node::DefineStmt(..)
                    | node::Node::DiscardStmt(..)
                    | node::Node::DoStmt(..)
                    | node::Node::DropOwnedStmt(..)
                    | node::Node::DropRoleStmt(..)
                    | node::Node::DropStmt(..)
                    | node::Node::DropSubscriptionStmt(..)
                    | node::Node::DropTableSpaceStmt(..)
                    | node::Node::DropUserMappingStmt(..)
                    | node::Node::DropdbStmt(..)
                    | node::Node::ExecuteStmt(..)
                    | node::Node::ExplainStmt(..)
                    | node::Node::FetchStmt(..)
                    | node::Node::FunctionParameter(..)
                    | node::Node::GrantRoleStmt(..)
                    | node::Node::GrantStmt(..)
                    | node::Node::ImportForeignSchemaStmt(..)
                    | node::Node::IntoClause(..)
                    | node::Node::IndexStmt(..)
                    | node::Node::InlineCodeBlock(..)
                    | node::Node::ListenStmt(..)
                    | node::Node::LoadStmt(..)
                    | node::Node::LockStmt(..)
                    | node::Node::LockingClause(..)
                    | node::Node::NotifyStmt(..)
                    | node::Node::PartitionBoundSpec(..)
                    | node::Node::PartitionCmd(..)
                    | node::Node::PartitionElem(..)
                    | node::Node::PartitionRangeDatum(..)
                    | node::Node::PartitionSpec(..)
                    | node::Node::PrepareStmt(..)
                    | node::Node::Query(..)
                    | node::Node::RangeTableFunc(..)
                    | node::Node::RangeTableFuncCol(..)
                    | node::Node::RangeTableSample(..)
                    | node::Node::RangeTblEntry(..)
                    | node::Node::RangeTblFunction(..)
                    | node::Node::RangeTblRef(..)
                    | node::Node::RawStmt(..)
                    | node::Node::ReassignOwnedStmt(..)
                    | node::Node::RefreshMatViewStmt(..)
                    | node::Node::ReindexStmt(..)
                    | node::Node::RenameStmt(..)
                    | node::Node::ReplicaIdentityStmt(..)
                    | node::Node::RoleSpec(..)
                    | node::Node::RowMarkClause(..)
                    | node::Node::RuleStmt(..)
                    | node::Node::SecLabelStmt(..)
                    | node::Node::SetOperationStmt(..)
                    | node::Node::SubscriptingRef(..)
                    | node::Node::SubPlan(..)
                    | node::Node::TableFunc(..)
                    | node::Node::TableLikeClause(..)
                    | node::Node::TableSampleClause(..)
                    | node::Node::TransactionStmt(..)
                    | node::Node::TriggerTransition(..)
                    | node::Node::TruncateStmt(..)
                    | node::Node::UnlistenStmt(..)
                    | node::Node::VacuumRelation(..)
                    | node::Node::VacuumStmt(..)
                    | node::Node::VariableSetStmt(..)
                    | node::Node::VariableShowStmt(..)
                    | node::Node::ViewStmt(..)
                    | node::Node::WithCheckOption(..)
                    | node::Node::XmlSerialize(..) => {
                        tracing::warn!(node = ?&node, "rejected query due to presence of banned node");

                        match self.allowed_statements {
                            AllowedStatements::All => (),
                            AllowedStatements::List(..) => return Err(Error::QueryRejected),
                        }
                    }
                    node::Node::FuncCall(function_call) => {
                        // test the function call recursively first
                        self.test_function_call(function_call)?;

                        // check the function call against allowed functions
                        let funcname = &function_call.funcname;

                        match &self.allowed_functions {
                            AllowedFunctions::All => (),
                            AllowedFunctions::List(functions) => {
                                // ban unnamed functions outright
                                let name =
                                    funcname.first().ok_or(Error::QueryRejected)?.node.as_ref();

                                match name {
                                    Some(node::Node::String(libpgquery_sys::String {
                                        str: name,
                                    })) => {
                                        // check the function name against the list
                                        if !functions.contains(name) {
                                            return Err(Error::FunctionNotAllowed(
                                                name.to_string(),
                                            ));
                                        }
                                    }
                                    // reject function names that aren't strings
                                    _ => return Err(Error::QueryRejected),
                                }
                            }
                        }
                    }
                    node::Node::Alias(..)
                    | node::Node::AStar(..)
                    | node::Node::BitString(..)
                    | node::Node::CallContext(..)
                    | node::Node::ColumnRef(..)
                    | node::Node::Expr(..)
                    | node::Node::Float(..)
                    | node::Node::Integer(..)
                    | node::Node::IntList(..)
                    | node::Node::Null(..)
                    | node::Node::OidList(..)
                    | node::Node::ParamRef(..)
                    | node::Node::RangeVar(..)
                    | node::Node::SortGroupClause(..)
                    | node::Node::String(..)
                    | node::Node::TargetEntry(..) => {
                        tracing::debug!(node = ?&node, "Skipping node");
                    }
                }
            }
        }

        Ok(())
    }
}
