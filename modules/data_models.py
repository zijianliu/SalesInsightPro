from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from enum import Enum


class DataType(Enum):
    ORDERS = "orders"
    PRODUCTS = "products"
    TARGETS = "targets"
    UNKNOWN = "unknown"


@dataclass
class FieldMapping:
    standard_name: str
    display_name: str
    possible_names: Set[str]
    data_type: str = "str"
    required: bool = True
    description: str = ""


class StandardFields:
    ORDERS_CORE: List[FieldMapping] = [
        FieldMapping(
            standard_name="订单日期",
            display_name="订单日期",
            possible_names={"订单日期", "下单时间", "交易日期", "日期", "date", "order_date", "transaction_date", "time"},
            data_type="date",
            required=True,
            description="订单发生的日期"
        ),
        FieldMapping(
            standard_name="订单号",
            display_name="订单号",
            possible_names={"订单号", "订单编号", "订单ID", "order_id", "order_no", "id"},
            data_type="str",
            required=False,
            description="唯一订单标识"
        ),
        FieldMapping(
            standard_name="产品名",
            display_name="产品名称",
            possible_names={"产品名", "产品名称", "商品名", "商品名称", "product", "product_name", "item", "item_name"},
            data_type="str",
            required=True,
            description="产品或商品名称"
        ),
        FieldMapping(
            standard_name="产品ID",
            display_name="产品ID",
            possible_names={"产品ID", "商品ID", "product_id", "item_id", "sku", "sku_id"},
            data_type="str",
            required=False,
            description="产品唯一标识"
        ),
        FieldMapping(
            standard_name="地区",
            display_name="销售地区",
            possible_names={"地区", "区域", "销售区域", "城市", "省份", "region", "area", "city", "province", "location"},
            data_type="str",
            required=False,
            description="销售发生的地区"
        ),
        FieldMapping(
            standard_name="类别",
            display_name="产品类别",
            possible_names={"类别", "分类", "品类", "产品类别", "category", "type", "product_type"},
            data_type="str",
            required=False,
            description="产品所属类别"
        ),
        FieldMapping(
            standard_name="销售额",
            display_name="销售金额",
            possible_names={"销售额", "销售金额", "金额", "收入", "营收", "sales", "sales_amount", "amount", "revenue", "price", "total"},
            data_type="numeric",
            required=True,
            description="销售金额"
        ),
        FieldMapping(
            standard_name="利润",
            display_name="利润金额",
            possible_names={"利润", "利润金额", "毛利", "profit", "margin", "gross_profit"},
            data_type="numeric",
            required=False,
            description="利润金额（如无则从商品表计算）"
        ),
        FieldMapping(
            standard_name="数量",
            display_name="销售数量",
            possible_names={"数量", "销售数量", "销量", "件数", "quantity", "qty", "count", "units"},
            data_type="numeric",
            required=False,
            description="销售数量"
        ),
        FieldMapping(
            standard_name="销售人员",
            display_name="销售人员",
            possible_names={"销售人员", "销售员", "销售代表", "业务员", "销售", "salesperson", "salesman", "sales_rep", "rep", "agent"},
            data_type="str",
            required=False,
            description="负责的销售人员"
        ),
        FieldMapping(
            standard_name="客户",
            display_name="客户名称",
            possible_names={"客户", "客户名", "客户名称", "customer", "client", "customer_name"},
            data_type="str",
            required=False,
            description="客户名称"
        ),
    ]

    PRODUCTS_CORE: List[FieldMapping] = [
        FieldMapping(
            standard_name="产品名",
            display_name="产品名称",
            possible_names={"产品名", "产品名称", "商品名", "商品名称", "product", "product_name", "item", "item_name"},
            data_type="str",
            required=True,
            description="产品或商品名称"
        ),
        FieldMapping(
            standard_name="产品ID",
            display_name="产品ID",
            possible_names={"产品ID", "商品ID", "product_id", "item_id", "sku", "sku_id"},
            data_type="str",
            required=False,
            description="产品唯一标识"
        ),
        FieldMapping(
            standard_name="类别",
            display_name="产品类别",
            possible_names={"类别", "分类", "品类", "产品类别", "category", "type", "product_type"},
            data_type="str",
            required=True,
            description="产品所属类别"
        ),
        FieldMapping(
            standard_name="成本",
            display_name="成本",
            possible_names={"成本", "成本价", "进货价", "cost", "cost_price", "purchase_price"},
            data_type="numeric",
            required=False,
            description="产品成本（用于计算利润）"
        ),
        FieldMapping(
            standard_name="售价",
            display_name="建议售价",
            possible_names={"售价", "建议售价", "标准价", "price", "standard_price", "list_price"},
            data_type="numeric",
            required=False,
            description="标准销售价格"
        ),
        FieldMapping(
            standard_name="品牌",
            display_name="品牌",
            possible_names={"品牌", "brand"},
            data_type="str",
            required=False,
            description="产品品牌"
        ),
    ]

    TARGETS_CORE: List[FieldMapping] = [
        FieldMapping(
            standard_name="年月",
            display_name="年月",
            possible_names={"年月", "月份", "日期", "年", "月", "year_month", "month", "period", "date"},
            data_type="date",
            required=True,
            description="目标所属年月"
        ),
        FieldMapping(
            standard_name="地区",
            display_name="销售地区",
            possible_names={"地区", "区域", "销售区域", "城市", "省份", "region", "area", "city", "province"},
            data_type="str",
            required=False,
            description="目标所属地区"
        ),
        FieldMapping(
            standard_name="类别",
            display_name="产品类别",
            possible_names={"类别", "分类", "品类", "产品类别", "category", "type", "product_type"},
            data_type="str",
            required=False,
            description="目标所属类别"
        ),
        FieldMapping(
            standard_name="销售人员",
            display_name="销售人员",
            possible_names={"销售人员", "销售员", "销售代表", "业务员", "销售", "salesperson", "salesman", "sales_rep"},
            data_type="str",
            required=False,
            description="目标所属销售人员"
        ),
        FieldMapping(
            standard_name="目标销售额",
            display_name="目标销售额",
            possible_names={"目标销售额", "销售目标", "目标金额", "target", "sales_target", "target_sales", "target_amount"},
            data_type="numeric",
            required=True,
            description="销售目标金额"
        ),
        FieldMapping(
            standard_name="目标利润",
            display_name="目标利润",
            possible_names={"目标利润", "利润目标", "target_profit", "profit_target"},
            data_type="numeric",
            required=False,
            description="利润目标金额"
        ),
    ]

    @classmethod
    def get_all_fields(cls) -> Dict[str, List[FieldMapping]]:
        return {
            "orders": cls.ORDERS_CORE,
            "products": cls.PRODUCTS_CORE,
            "targets": cls.TARGETS_CORE,
        }

    @classmethod
    def infer_data_type(cls, columns: List[str]) -> DataType:
        col_set = {c.lower() for c in columns}
        
        orders_signals = {"订单日期", "下单时间", "date", "order_date", "transaction_date", "销售额", "sales", "amount", "revenue", "订单号", "order_id", "产品", "product"}
        orders_match = sum(1 for s in orders_signals if any(s in c for c in col_set))
        
        products_signals = {"类别", "category", "成本", "cost", "品牌", "brand", "商品", "sku", "售价", "price"}
        products_match = sum(1 for s in products_signals if any(s in c for c in col_set))
        
        targets_signals = {"目标", "target", "销售目标", "目标销售额", "goal", "quota", "budget", "年月", "月份", "year_month", "month", "period"}
        targets_match = sum(1 for s in targets_signals if any(s in c for c in col_set))
        
        if targets_match >= 2:
            return DataType.TARGETS
        if products_match >= 2 and orders_match < 2:
            return DataType.PRODUCTS
        if orders_match >= 2:
            return DataType.ORDERS
        return DataType.UNKNOWN

    @classmethod
    def map_columns(cls, columns: List[str], data_type: DataType) -> Dict[str, str]:
        mapping = {}
        fields = []
        
        if data_type == DataType.ORDERS:
            fields = cls.ORDERS_CORE
        elif data_type == DataType.PRODUCTS:
            fields = cls.PRODUCTS_CORE
        elif data_type == DataType.TARGETS:
            fields = cls.TARGETS_CORE
        
        col_lower = {c.lower(): c for c in columns}
        
        for field in fields:
            matched = None
            for possible_name in field.possible_names:
                if possible_name.lower() in col_lower:
                    matched = col_lower[possible_name.lower()]
                    break
            
            if matched:
                mapping[matched] = field.standard_name
            elif field.required:
                pass
        
        for col in columns:
            if col not in mapping:
                mapping[col] = col
        
        return mapping


@dataclass
class DatasetInfo:
    dataset_id: str
    data_type: DataType
    file_name: str
    column_mapping: Dict[str, str]
    row_count: int
    sample_data: Dict[str, Any] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)


@dataclass
class JoinKey:
    left_dataset: str
    right_dataset: str
    left_columns: List[str]
    right_columns: List[str]
    join_type: str = "left"


@dataclass
class AnalysisResult:
    success: bool
    message: str
    data: Any = None
    warnings: List[str] = field(default_factory=list)
