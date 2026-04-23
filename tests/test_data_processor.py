import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules import (
    AdvancedDataProcessor, DataCleaner,
    StandardFields, DataType, AnalysisResult
)


class TestDataCleaner:
    def test_parse_date_various_formats(self):
        assert DataCleaner.parse_date('2024-01-15') == datetime(2024, 1, 15)
        assert DataCleaner.parse_date('2024/01/15') == datetime(2024, 1, 15)
        assert DataCleaner.parse_date('2024.01.15') == datetime(2024, 1, 15)
        assert DataCleaner.parse_date('2024年01月15日') == datetime(2024, 1, 15)
        assert DataCleaner.parse_date('15-01-2024') == datetime(2024, 1, 15)
        assert DataCleaner.parse_date('01/15/2024') == datetime(2024, 1, 15)

    def test_parse_date_year_month(self):
        assert DataCleaner.parse_date('2024-01') is not None
        assert DataCleaner.parse_date('2024/01') is not None
        assert DataCleaner.parse_date('2024年01月') is not None
        assert DataCleaner.parse_date('202401') is not None

    def test_parse_date_none_and_invalid(self):
        assert DataCleaner.parse_date(None) is None
        assert DataCleaner.parse_date('') is None
        assert DataCleaner.parse_date('无效日期') is None
        assert DataCleaner.parse_date(np.nan) is None

    def test_parse_numeric_basic(self):
        assert DataCleaner.parse_numeric('1000') == 1000.0
        assert DataCleaner.parse_numeric('1,000') == 1000.0
        assert DataCleaner.parse_numeric('1.5') == 1.5
        assert DataCleaner.parse_numeric(100) == 100.0
        assert DataCleaner.parse_numeric(100.5) == 100.5

    def test_parse_numeric_with_currency(self):
        assert DataCleaner.parse_numeric('¥1000') == 1000.0
        assert DataCleaner.parse_numeric('￥1000') == 1000.0
        assert DataCleaner.parse_numeric('$1000') == 1000.0
        assert DataCleaner.parse_numeric('¥1,000.50') == 1000.5

    def test_parse_numeric_with_chinese_units(self):
        assert DataCleaner.parse_numeric('1万') == 10000.0
        assert DataCleaner.parse_numeric('1.5万') == 15000.0
        assert DataCleaner.parse_numeric('1亿') == 100000000.0
        assert DataCleaner.parse_numeric('¥2.5万') == 25000.0

    def test_parse_numeric_none_and_invalid(self):
        assert DataCleaner.parse_numeric(None) is None
        assert DataCleaner.parse_numeric('') is None
        assert DataCleaner.parse_numeric('无效数据') is None
        assert DataCleaner.parse_numeric(np.nan) is None


class TestStandardFields:
    def test_infer_data_type_orders(self):
        orders_columns = ['订单日期', '销售额', '产品名', '地区']
        assert StandardFields.infer_data_type(orders_columns) == DataType.ORDERS
        
        orders_columns_en = ['date', 'sales', 'product_name', 'region']
        assert StandardFields.infer_data_type(orders_columns_en) == DataType.ORDERS

    def test_infer_data_type_products(self):
        products_columns = ['产品名', '类别', '成本', '品牌']
        assert StandardFields.infer_data_type(products_columns) == DataType.PRODUCTS
        
        products_columns_en = ['product_name', 'category', 'cost', 'brand']
        assert StandardFields.infer_data_type(products_columns_en) == DataType.PRODUCTS

    def test_infer_data_type_targets(self):
        targets_columns = ['年月', '目标销售额', '地区']
        assert StandardFields.infer_data_type(targets_columns) == DataType.TARGETS
        
        targets_columns_en = ['month', 'sales_target', 'region']
        assert StandardFields.infer_data_type(targets_columns_en) == DataType.TARGETS

    def test_map_columns_orders(self):
        columns = ['订单日期', '销售金额', '商品名', '区域', 'profit']
        mapping = StandardFields.map_columns(columns, DataType.ORDERS)
        
        assert mapping.get('订单日期') == '订单日期'
        assert mapping.get('销售金额') == '销售额'
        assert mapping.get('商品名') == '产品名'
        assert mapping.get('区域') == '地区'
        assert mapping.get('profit') == '利润'

    def test_map_columns_products(self):
        columns = ['商品名', '分类', '成本价', '品牌']
        mapping = StandardFields.map_columns(columns, DataType.PRODUCTS)
        
        assert mapping.get('商品名') == '产品名'
        assert mapping.get('分类') == '类别'
        assert mapping.get('成本价') == '成本'
        assert mapping.get('品牌') == '品牌'


class TestAdvancedDataProcessor:
    @pytest.fixture
    def sample_orders_df(self):
        data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': '2024-01-18', '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '2024-01-22', '地区': '华东', '类别': '电子产品', '产品名': 'MacBook Pro', '销售额': 14999, '利润': 3500, '销售人员': '张三'},
            {'订单日期': '2024-02-05', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
        ]
        return pd.DataFrame(data)

    @pytest.fixture
    def sample_products_df(self):
        data = [
            {'产品名': 'iPhone 15', '类别': '电子产品', '成本': 6500, '品牌': 'Apple'},
            {'产品名': 'MacBook Pro', '类别': '电子产品', '成本': 11000, '品牌': 'Apple'},
            {'产品名': 'Nike运动鞋', '类别': '服装', '成本': 600, '品牌': 'Nike'},
        ]
        return pd.DataFrame(data)

    @pytest.fixture
    def sample_targets_df(self):
        data = [
            {'年月': '2024-01', '地区': '', '目标销售额': 30000, '目标利润': 7500},
            {'年月': '2024-02', '地区': '', '目标销售额': 20000, '目标利润': 5000},
        ]
        return pd.DataFrame(data)

    def test_calculate_metrics(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        result = processor.load_dataset(csv_buffer, 'test_orders.csv')
        assert result.success
        
        result = processor.process_and_unify()
        assert result.success
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        
        metrics = processor.calculate_metrics(unified_df)
        
        assert metrics['total_sales'] == 8999 + 899 + 14999 + 4599
        assert metrics['total_profit'] == 2100 + 180 + 3500 + 920
        assert metrics['order_count'] == 4
        assert metrics['profit_rate'] == (2100 + 180 + 3500 + 920) / (8999 + 899 + 14999 + 4599)

    def test_filter_data_by_region(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        filtered = processor.filter_data(regions=['华东'])
        
        assert len(filtered) == 2
        assert all(filtered['地区'] == '华东')

    def test_filter_data_by_category(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        filtered = processor.filter_data(categories=['电子产品'])
        
        assert len(filtered) == 2
        assert all(filtered['类别'] == '电子产品')

    def test_analyze_by_dimension(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        unified_df = processor.get_unified_data()
        region_data = processor.analyze_by_dimension(unified_df, '地区')
        
        assert '销售额' in region_data.columns
        assert '利润' in region_data.columns
        assert '销售额占比' in region_data.columns
        assert '利润率' in region_data.columns
        
        assert len(region_data) == 3

    def test_analyze_trends(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        unified_df = processor.get_unified_data()
        trend_data = processor.analyze_trends(unified_df)
        
        assert 'period' in trend_data.columns
        assert '销售额' in trend_data.columns
        assert '利润' in trend_data.columns
        assert '销售额环比' in trend_data.columns

    def test_calculate_target_achievement_without_targets(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        unified_df = processor.get_unified_data()
        target_info = processor.calculate_target_achievement(unified_df)
        
        assert target_info['has_target'] == False
        assert target_info['achievement_rate'] == 0

    def test_get_unique_values(self, sample_orders_df):
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        sample_orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        unique_values = processor.get_unique_values()
        
        assert 'regions' in unique_values
        assert 'categories' in unique_values
        assert 'salespeople' in unique_values
        assert 'date_range' in unique_values
        
        assert '华东' in unique_values['regions']
        assert '电子产品' in unique_values['categories']
        assert '张三' in unique_values['salespeople']

    def test_data_cleaning_stats(self, sample_orders_df):
        df_with_duplicates = pd.concat([sample_orders_df, sample_orders_df.head(1)], ignore_index=True)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        df_with_duplicates.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        stats = processor.get_cleaning_stats()
        
        assert stats['original_count'] == 5
        assert stats['duplicates_removed'] == 1
        assert stats['final_count'] == 4


class TestIntegration:
    def test_full_flow_with_multiple_datasets(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '数量': 1, '销售人员': '张三'},
            {'订单日期': '2024-01-18', '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '数量': 2, '销售人员': '李四'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        products_data = [
            {'产品名': 'iPhone 15', '类别': '电子产品', '成本': 6500, '品牌': 'Apple'},
            {'产品名': 'Nike运动鞋', '类别': '服装', '成本': 600, '品牌': 'Nike'},
        ]
        products_df = pd.DataFrame(products_data)
        
        targets_data = [
            {'年月': '2024-01', '地区': '', '目标销售额': 20000, '目标利润': 5000},
        ]
        targets_df = pd.DataFrame(targets_data)
        
        processor = AdvancedDataProcessor()
        
        orders_csv = BytesIO()
        orders_df.to_csv(orders_csv, index=False)
        orders_csv.seek(0)
        processor.load_dataset(orders_csv, 'orders.csv')
        
        products_csv = BytesIO()
        products_df.to_csv(products_csv, index=False)
        products_csv.seek(0)
        processor.load_dataset(products_csv, 'products.csv')
        
        targets_csv = BytesIO()
        targets_df.to_csv(targets_csv, index=False)
        targets_csv.seek(0)
        processor.load_dataset(targets_csv, 'targets.csv')
        
        result = processor.process_and_unify()
        
        assert result.success
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        
        assert '品牌' in unified_df.columns
        
        metrics = processor.calculate_metrics(unified_df)
        assert metrics['total_sales'] == 8999 + 899
        
        target_info = processor.calculate_target_achievement(unified_df)
        assert target_info['has_target'] == True


class TestDirtyDataScenarios:
    def test_with_null_dates(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': None, '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        
        result = processor.process_and_unify()
        
        assert result.success
        
        stats = processor.get_cleaning_stats()
        assert stats['null_rows_removed'] == 2
        assert stats['final_count'] == 1
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        assert len(unified_df) == 1
        assert unified_df['订单日期'].iloc[0] == pd.Timestamp('2024-01-15')

    def test_with_invalid_dates(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': '无效日期', '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '2024/13/01', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
            {'订单日期': 'abcdef', '地区': '西南', '类别': '食品饮料', '产品名': '蒙牛牛奶', '销售额': 68, '利润': 14, '销售人员': '赵六'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        
        result = processor.process_and_unify()
        
        assert result.success
        
        stats = processor.get_cleaning_stats()
        assert stats['null_rows_removed'] == 3
        assert stats['final_count'] == 1
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        assert len(unified_df) == 1

    def test_mixed_valid_and_invalid_dates(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': '2024/02/20', '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '无效日期', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
            {'订单日期': '2024年3月10日', '地区': '西南', '类别': '食品饮料', '产品名': '蒙牛牛奶', '销售额': 68, '利润': 14, '销售人员': '赵六'},
            {'订单日期': '', '地区': '西北', '类别': '美妆护肤', '产品名': '兰蔻小黑瓶', '销售额': 1080, '利润': 270, '销售人员': '钱七'},
            {'订单日期': '15-04-2024', '地区': '东北', '类别': '电子产品', '产品名': 'MacBook Pro', '销售额': 14999, '利润': 3500, '销售人员': '孙八'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        
        result = processor.process_and_unify()
        
        assert result.success
        
        stats = processor.get_cleaning_stats()
        assert stats['null_rows_removed'] == 2
        assert stats['final_count'] == 4
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        assert len(unified_df) == 4
        
        unique_values = processor.get_unique_values()
        assert 'date_range' in unique_values
        assert 'years' in unique_values
        assert 2024 in unique_values['years']

    def test_time_columns_with_nulls(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': '2024-02-20', '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '无效日期', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        
        assert '年份' in unified_df.columns
        assert '月份' in unified_df.columns
        assert '年月' in unified_df.columns
        assert '季度' in unified_df.columns
        assert '周' in unified_df.columns
        
        assert unified_df['年份'].dtype == 'Int64'
        assert unified_df['月份'].dtype == 'Int64'
        assert unified_df['季度'].dtype == 'Int64'
        assert unified_df['周'].dtype == 'Int64'
        
        assert 2024 in unified_df['年份'].values
        assert 1 in unified_df['月份'].values
        assert 2 in unified_df['月份'].values

    def test_empty_date_column(self):
        orders_data = [
            {'订单日期': '', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': None, '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '无效', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        result = processor.process_and_unify()
        
        assert result.success
        
        stats = processor.get_cleaning_stats()
        assert stats['null_rows_removed'] == 3
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        assert len(unified_df) == 0

    def test_get_unique_values_with_missing_dates(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': None, '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '2024-03-20', '地区': '华东', '类别': '电子产品', '产品名': 'MacBook Pro', '销售额': 14999, '利润': 3500, '销售人员': '张三'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        processor.process_and_unify()
        
        stats = processor.get_cleaning_stats()
        assert stats['null_rows_removed'] == 1
        
        unique_values = processor.get_unique_values()
        
        assert 'date_range' in unique_values
        assert 'years' in unique_values
        assert 2024 in unique_values['years']
        assert 'regions' in unique_values
        assert '华东' in unique_values['regions']
        assert '华北' not in unique_values['regions']

    def test_various_date_formats(self):
        orders_data = [
            {'订单日期': '2024-01-15', '地区': '华东', '类别': '电子产品', '产品名': 'iPhone 15', '销售额': 8999, '利润': 2100, '销售人员': '张三'},
            {'订单日期': '2024/02/20', '地区': '华北', '类别': '服装', '产品名': 'Nike运动鞋', '销售额': 899, '利润': 180, '销售人员': '李四'},
            {'订单日期': '2024年3月10日', '地区': '华南', '类别': '家居用品', '产品名': '美的空调', '销售额': 4599, '利润': 920, '销售人员': '王五'},
            {'订单日期': '15-04-2024', '地区': '西南', '类别': '食品饮料', '产品名': '蒙牛牛奶', '销售额': 68, '利润': 14, '销售人员': '赵六'},
            {'订单日期': '05/20/2024', '地区': '西北', '类别': '美妆护肤', '产品名': '兰蔻小黑瓶', '销售额': 1080, '利润': 270, '销售人员': '钱七'},
        ]
        orders_df = pd.DataFrame(orders_data)
        
        processor = AdvancedDataProcessor()
        
        csv_buffer = BytesIO()
        orders_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        processor.load_dataset(csv_buffer, 'test_orders.csv')
        result = processor.process_and_unify()
        
        assert result.success
        
        stats = processor.get_cleaning_stats()
        assert stats['null_rows_removed'] == 0
        assert stats['final_count'] == 5
        
        unified_df = processor.get_unified_data()
        assert unified_df is not None
        assert len(unified_df) == 5
        
        metrics = processor.calculate_metrics(unified_df)
        assert metrics['total_sales'] == 8999 + 899 + 4599 + 68 + 1080


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
