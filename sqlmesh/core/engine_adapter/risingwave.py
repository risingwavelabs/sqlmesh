from __future__ import annotations

import logging
import typing as t
from sqlglot import Dialect, exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import set_catalog
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.core.dialect import to_schema

from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName, SchemaName, SessionProperties
    from sqlmesh.core.engine_adapter._typing import DF
    from sqlmesh.core.engine_adapter._typing import QueryOrDF

logger = logging.getLogger(__name__)


@set_catalog()
class RisingWaveEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    # GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "risingwave"
    SUPPORTS_INDEXES = True
    HAS_VIEW_BINDING = True
    # CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    SUPPORTS_REPLACE_TABLE = False
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            # DECIMAL without precision is "up to 131072 digits before the decimal point; up to 16383 digits after the decimal point"
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(131072 + 16383, 16383), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("TIME", dialect=DIALECT).this: [(6,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(6,)],
        },
        types_with_unlimited_length={
            # all can ALTER to `TEXT`
            exp.DataType.build("TEXT", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("BPCHAR", dialect=DIALECT).this,
            },
            # all can ALTER to unparameterized `VARCHAR`
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("BPCHAR", dialect=DIALECT).this,
                exp.DataType.build("TEXT", dialect=DIALECT).this,
            },
            # parameterized `BPCHAR(n)` can ALTER to unparameterized `BPCHAR`
            exp.DataType.build("BPCHAR", dialect=DIALECT).this: {
                exp.DataType.build("BPCHAR", dialect=DIALECT).this
            },
        },
    )
    
    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str = "",
        sql_gen_kwargs: t.Optional[t.Dict[str, Dialect | bool | str]] = None,
        multithreaded: bool = False,
        cursor_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        cursor_init: t.Optional[t.Callable[[t.Any], None]] = None,
        default_catalog: t.Optional[str] = None,
        execute_log_level: int = logging.DEBUG,
        register_comments: bool = True,
        pre_ping: bool = False,
        **kwargs: t.Any,
    ):
        super().__init__(connection_factory, dialect, sql_gen_kwargs, multithreaded, cursor_kwargs, cursor_init, default_catalog, execute_log_level, register_comments, pre_ping, **kwargs)
        try:
            sql = "SET RW_IMPLICIT_FLUSH TO true;"
            print("*" * 100)
            print("Executing rw implicit flush")
            print("*" * 100)
            self._execute(sql)
        except Exception as e:
            print("-" * 100)
            print("Error executing rw implicit flush")
            print(e)
            print("-" * 100)
    
    def _begin_session(self, properties: SessionProperties) -> t.Any:
        """Begin a new session."""
        sql = "SET RW_IMPLICIT_FLUSH TO true;"
        print("*" * 100)
        print("Executing rw implicit flush")
        print("*" * 100)
        self._execute(sql)
        

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """
        `read_sql_query` when using psycopg will result on a hanging transaction that must be committed

        https://github.com/pandas-dev/pandas/pull/42277
        """
        df = super()._fetch_native_df(query, quote_identifiers)
        if not self._connection_pool.is_transaction_active:
            self._connection_pool.commit()
        return df

    def create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        self.execute(
            exp.Create(
                this=exp.Schema(
                    this=exp.to_table(target_table_name),
                    expressions=[
                        exp.LikeProperty(
                            this=exp.to_table(source_table_name),
                            expressions=[exp.Property(this="INCLUDING", value=exp.Var(this="ALL"))],
                        )
                    ],
                ),
                kind="TABLE",
                exists=exists,
            )
        )

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog = None #self.get_current_catalog()
        table_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("tablename").as_("name"),
            exp.Literal.string("TABLE").as_("type"),
        ).from_("pg_tables")
        view_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("viewname").as_("name"),
            exp.Literal.string("VIEW").as_("type"),
        ).from_("pg_views")
        materialized_view_query = exp.select(
            exp.column("schemaname").as_("schema_name"),
            exp.column("matviewname").as_("name"),
            exp.Literal.string("MATERIALIZED_VIEW").as_("type"),
        ).from_("pg_matviews")
        subquery = exp.union(
            table_query,
            exp.union(view_query, materialized_view_query, distinct=False),
            distinct=False,
        )
        query = (
            exp.select("*")
            .from_(subquery.subquery(alias="objs"))
            .where(exp.column("schema_name").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=catalog,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]