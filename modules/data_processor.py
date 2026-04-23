import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any, List, Optional, Union
from datetime import datetime
from fuzzywuzzy import fuzz
import re

from .data_models import (
    DataType, StandardFields, DatasetInfo, AnalysisResult
)


class DataCleaner:
    DATE_FORMATS = [
        '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
        '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',
        '%m-%d-%Y', '%m/%d/%Y', '%m.%d.%Y',
        '%Y年%m月%d日', '%Y年%m月', '%Y-%m',
        '%Y/%m', '%Y.%m',
    ]

    @classmethod
    def parse_date(cls, date_str: str) -> Optional[datetime]:
        if pd.isna(date_str) or date_str is None:
            return None
        
        date_str = str(date_str).strip()
        
        if re.match(r'^\d{4}$', date_str):
            return datetime.strptime(date_str, '%Y')
        
        if re.match(r'^\d{6}$', date_str):
            try:
                return datetime.strptime(date_str, '%Y%m')
            except:
                pass
        
        for fmt in cls.DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        try:
            return pd.to_datetime(date_str, errors='raise').to_pydatetime()
        except:
            return None

    @classmethod
    def parse_numeric(cls, value: Any) -> Optional[float]:
        if pd.isna(value) or value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        value_str = str(value).strip()
        
        value_str = re.sub(r'[¥￥$€£,]', '', value_str)
        
        value_str = value_str.replace('万', '0000').replace('亿', '00000000')
        
        try:
            return float(value_str)
        except ValueError:
            return None

    @classmethod
    def fuzzy_match(cls, value: str, candidates: List[str], threshold: int = 80) -> Optional[str]:
        if not value or not candidates:
            return None
        
        value_lower = str(value).strip().lower()
        best_match = None
        best_score = 0
        
        for candidate in candidates:
            candidate_lower = str(candidate).strip().lower()
            
            score = max(
                fuzz.ratio(value_lower, candidate_lower),
                fuzz.partial_ratio(value_lower, candidate_lower),
                fuzz.token_sort_ratio(value_lower, candidate_lower)
            )
            
            if score > best_score and score >= threshold:
                best_score = score
                best_match = candidate
        
        return best_match


class AdvancedDataProcessor:
    def __init__(self):
        self.datasets: Dict[str, pd.DataFrame] = {}
        self.dataset_info: Dict[str, DatasetInfo] = {}
        self._unified_data: Optional[pd.DataFrame] = None
        self._target_data: Optional[pd.DataFrame] = None
        self._products_data: Optional[pd.DataFrame] = None
        self._cleaning_stats: Dict[str, Any] = {}

    def load_dataset(self, file_path, file_name: str) -> AnalysisResult:
        try:
            if file_path is None:
                return AnalysisResult(success=False, message="请上传文件")

            df = pd.read_csv(file_path)
            
            columns = df.columns.tolist()
            data_type = StandardFields.infer_data_type(columns)
            
            if data_type == DataType.UNKNOWN:
                return AnalysisResult(
                    success=False,
                    message="无法识别数据表类型，请检查表头字段"
                )
            
            mapping = StandardFields.map_columns(columns, data_type)
            
            df_renamed = df.rename(columns=mapping)
            
            dataset_id = f"{data_type.value}_{len(self.datasets) + 1}"
            
            issues = self._validate_dataset(df_renamed, data_type)
            
            sample_data = {}
            if not df_renamed.empty:
                sample_row = df_renamed.head(1).fillna('').to_dict('records')[0]
                sample_data = {k: str(v)[:50] for k, v in sample_row.items()}
            
            self.datasets[dataset_id] = df_renamed
            self.dataset_info[dataset_id] = DatasetInfo(
                dataset_id=dataset_id,
                data_type=data_type,
                file_name=file_name,
                column_mapping=mapping,
                row_count=len(df_renamed),
                sample_data=sample_data,
                issues=issues
            )
            
            return AnalysisResult(
                success=True,
                message=f"成功加载 {self._get_type_name(data_type)} 数据，共 {len(df_renamed)} 条记录",
                data={"dataset_id": dataset_id, "data_type": data_type.value}
            )
            
        except Exception as e:
            return AnalysisResult(success=False, message=f"数据加载失败: {str(e)}")

    def _get_type_name(self, data_type: DataType) -> str:
        names = {
            DataType.ORDERS: "订单",
            DataType.PRODUCTS: "商品",
            DataType.TARGETS: "目标",
        }
        return names.get(data_type, "未知类型")

    def _validate_dataset(self, df: pd.DataFrame, data_type: DataType) -> List[str]:
        issues = []
        
        if data_type == DataType.ORDERS:
            if '订单日期' not in df.columns:
                issues.append("缺少必需字段：订单日期")
            if '销售额' not in df.columns:
                issues.append("缺少必需字段：销售额")
            if '产品名' not in df.columns:
                issues.append("缺少建议字段：产品名（将影响与商品表关联）")
        
        elif data_type == DataType.PRODUCTS:
            if '产品名' not in df.columns and '产品ID' not in df.columns:
                issues.append("缺少必需字段：产品名或产品ID")
        
        elif data_type == DataType.TARGETS:
            if '年月' not in df.columns:
                issues.append("缺少必需字段：年月")
            if '目标销售额' not in df.columns:
                issues.append("缺少必需字段：目标销售额")
        
        null_counts = df.isnull().sum()
        high_null_cols = null_counts[null_counts > len(df) * 0.3]
        for col, count in high_null_cols.items():
            issues.append(f"字段 '{col}' 有 {count} 个空值（占比 {count/len(df)*100:.1f}%）")
        
        return issues

    def get_datasets_by_type(self, data_type: DataType) -> List[str]:
        return [
            dataset_id 
            for dataset_id, info in self.dataset_info.items()
            if info.data_type == data_type
        ]

    def process_and_unify(self) -> AnalysisResult:
        warnings = []
        
        orders_ids = self.get_datasets_by_type(DataType.ORDERS)
        if not orders_ids:
            return AnalysisResult(
                success=False,
                message="请至少上传一份订单数据"
            )
        
        all_orders = []
        for oid in orders_ids:
            df = self._clean_orders_data(self.datasets[oid])
            all_orders.append(df)
        
        unified = pd.concat(all_orders, ignore_index=True)
        
        self._cleaning_stats = {
            'original_count': len(unified),
            'duplicates_removed': 0,
            'null_rows_removed': 0,
            'final_count': 0
        }
        
        duplicates = unified.duplicated()
        if duplicates.any():
            self._cleaning_stats['duplicates_removed'] = duplicates.sum()
            unified = unified.drop_duplicates()
        
        products_ids = self.get_datasets_by_type(DataType.PRODUCTS)
        if products_ids:
            products_df = self._clean_products_data(self.datasets[products_ids[0]])
            self._products_data = products_df
            unified = self._join_with_products(unified, products_df)
            warnings.append("已关联商品表信息")
        
        targets_ids = self.get_datasets_by_type(DataType.TARGETS)
        if targets_ids:
            targets_df = self._clean_targets_data(self.datasets[targets_ids[0]])
            self._target_data = targets_df
            warnings.append("已加载目标数据，将用于目标达成分析")
        
        if '利润' not in unified.columns:
            if '成本' in unified.columns and '数量' in unified.columns:
                unified['利润'] = (unified['销售额'] - unified['成本'] * unified['数量'])
            elif '成本' in unified.columns:
                unified['利润'] = unified['销售额'] * 0.2
        
        if '利润' not in unified.columns:
            unified['利润'] = unified['销售额'] * 0.2
            warnings.append("无法计算利润，使用默认利润率20%估算")
        
        unified = self._add_time_columns(unified)
        
        null_mask = unified[['订单日期', '销售额']].isnull().any(axis=1)
        if null_mask.any():
            self._cleaning_stats['null_rows_removed'] = null_mask.sum()
            unified = unified[~null_mask]
        
        self._cleaning_stats['final_count'] = len(unified)
        self._unified_data = unified.reset_index(drop=True)
        
        return AnalysisResult(
            success=True,
            message=f"数据处理完成，共 {len(self._unified_data)} 条有效记录",
            warnings=warnings
        )

    def _clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        
        if '订单日期' in result.columns:
            result['订单日期'] = result['订单日期'].apply(DataCleaner.parse_date)
        
        for col in ['销售额', '利润', '数量', '成本']:
            if col in result.columns:
                result[col] = result[col].apply(DataCleaner.parse_numeric)
        
        for col in ['产品名', '地区', '类别', '销售人员']:
            if col in result.columns:
                result[col] = result[col].fillna('未知').astype(str).str.strip()
        
        return result

    def _clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        
        for col in ['成本', '售价']:
            if col in result.columns:
                result[col] = result[col].apply(DataCleaner.parse_numeric)
        
        for col in ['产品名', '产品ID', '类别', '品牌']:
            if col in result.columns:
                result[col] = result[col].fillna('').astype(str).str.strip()
        
        return result

    def _clean_targets_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        
        if '年月' in result.columns:
            result['年月'] = result['年月'].apply(DataCleaner.parse_date)
            result['年月'] = result['年月'].apply(lambda x: x.strftime('%Y-%m') if x else None)
        
        for col in ['目标销售额', '目标利润']:
            if col in result.columns:
                result[col] = result[col].apply(DataCleaner.parse_numeric)
        
        for col in ['地区', '类别', '销售人员']:
            if col in result.columns:
                result[col] = result[col].fillna('').astype(str).str.strip()
        
        return result

    def _join_with_products(self, orders_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        result = orders_df.copy()
        
        join_keys = []
        if '产品ID' in result.columns and '产品ID' in products_df.columns:
            join_keys.append('产品ID')
        if '产品名' in result.columns and '产品名' in products_df.columns:
            join_keys.append('产品名')
        
        if not join_keys:
            return result
        
        products_cols = [c for c in products_df.columns if c not in result.columns or c in join_keys]
        products_subset = products_df[products_cols].copy()
        
        result = pd.merge(
            result,
            products_subset,
            on=join_keys,
            how='left',
            suffixes=('', '_product')
        )
        
        return result

    def _add_time_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        
        if '订单日期' in result.columns:
            result['年份'] = result['订单日期'].dt.year
            result['月份'] = result['订单日期'].dt.month
            result['年月'] = result['订单日期'].dt.to_period('M').astype(str)
            result['季度'] = result['订单日期'].dt.quarter
            result['周'] = result['订单日期'].dt.isocalendar().week.astype(int)
        
        return result

    def get_cleaning_stats(self) -> Dict[str, Any]:
        return self._cleaning_stats.copy()

    def get_unified_data(self) -> Optional[pd.DataFrame]:
        return self._unified_data

    def get_target_data(self) -> Optional[pd.DataFrame]:
        return self._target_data

    def get_products_data(self) -> Optional[pd.DataFrame]:
        return self._products_data

    def get_unique_values(self) -> Dict[str, Any]:
        if self._unified_data is None or self._unified_data.empty:
            return {}
        
        result = {
            'date_range': (self._unified_data['订单日期'].min(), self._unified_data['订单日期'].max()),
            'years': sorted(self._unified_data['年份'].unique().tolist()),
            'months': sorted(self._unified_data['月份'].unique().tolist()),
            'year_months': sorted(self._unified_data['年月'].unique().tolist()),
        }
        
        for col in ['地区', '类别', '销售人员', '品牌']:
            if col in self._unified_data.columns:
                unique_vals = sorted(self._unified_data[col].dropna().unique().tolist())
                unique_vals = [v for v in unique_vals if v not in ['', '未知']]
                if unique_vals:
                    result[col.lower() + 's'] = unique_vals
        
        return result

    def filter_data(
        self,
        date_range: Tuple[datetime, datetime] = None,
        years: List[int] = None,
        months: List[int] = None,
        regions: List[str] = None,
        categories: List[str] = None,
        salespeople: List[str] = None,
        brands: List[str] = None,
    ) -> pd.DataFrame:
        if self._unified_data is None:
            return pd.DataFrame()

        df = self._unified_data.copy()

        if date_range:
            start_date, end_date = date_range
            df = df[(df['订单日期'] >= pd.Timestamp(start_date)) & 
                    (df['订单日期'] <= pd.Timestamp(end_date))]

        if years and len(years) > 0:
            df = df[df['年份'].isin(years)]

        if months and len(months) > 0:
            df = df[df['月份'].isin(months)]

        if regions and len(regions) > 0:
            df = df[df['地区'].isin(regions)]

        if categories and len(categories) > 0:
            df = df[df['类别'].isin(categories)]

        if salespeople and len(salespeople) > 0:
            df = df[df['销售人员'].isin(salespeople)]

        if brands and len(brands) > 0:
            df = df[df['品牌'].isin(brands)]

        return df

    def calculate_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty:
            return {
                'total_sales': 0,
                'total_profit': 0,
                'order_count': 0,
                'avg_order_sales': 0,
                'profit_rate': 0,
                'total_quantity': 0,
            }

        total_sales = df['销售额'].sum()
        total_profit = df['利润'].sum() if '利润' in df.columns else 0
        
        metrics = {
            'total_sales': total_sales,
            'total_profit': total_profit,
            'order_count': len(df),
            'avg_order_sales': df['销售额'].mean(),
            'profit_rate': total_profit / total_sales if total_sales > 0 else 0,
            'total_quantity': df['数量'].sum() if '数量' in df.columns else len(df),
        }
        
        return metrics

    def calculate_target_achievement(self, df: pd.DataFrame) -> Dict[str, Any]:
        if self._target_data is None or self._target_data.empty:
            return {
                'has_target': False,
                'achievement_rate': 0,
                'target_sales': 0,
                'actual_sales': 0,
                'gap': 0,
            }
        
        actual_sales = df['销售额'].sum() if not df.empty else 0
        
        target_df = self._target_data.copy()
        target_sales = target_df['目标销售额'].sum()
        
        unique_dims = []
        for col in ['地区', '类别', '销售人员']:
            if col in target_df.columns:
                unique_dims.append(col)
        
        if unique_dims:
            period_col = '年月' if '年月' in target_df.columns else None
            
            if period_col and period_col in df.columns:
                target_by_period = target_df.groupby(period_col)['目标销售额'].sum()
                actual_by_period = df.groupby(period_col)['销售额'].sum()
                
                common_periods = set(target_by_period.index) & set(actual_by_period.index)
                if common_periods:
                    target_sales = target_by_period.loc[list(common_periods)].sum()
                    actual_sales = actual_by_period.loc[list(common_periods)].sum()
        
        achievement_rate = actual_sales / target_sales if target_sales > 0 else 0
        
        return {
            'has_target': True,
            'achievement_rate': achievement_rate,
            'target_sales': target_sales,
            'actual_sales': actual_sales,
            'gap': target_sales - actual_sales,
        }

    def analyze_trends(self, df: pd.DataFrame, freq: str = 'M') -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        
        df_copy = df.copy()
        
        if freq == 'M':
            df_copy['period'] = df_copy['年月']
        elif freq == 'Q':
            df_copy['period'] = df_copy.apply(lambda x: f"{x['年份']}Q{x['季度']}", axis=1)
        elif freq == 'W':
            df_copy['period'] = df_copy.apply(lambda x: f"{x['年份']}W{x['周']}", axis=1)
        else:
            df_copy['period'] = df_copy['年月']
        
        trend_data = df_copy.groupby('period').agg({
            '销售额': 'sum',
            '利润': 'sum',
        }).reset_index()
        
        trend_data = trend_data.sort_values('period')
        
        trend_data['销售额环比'] = trend_data['销售额'].pct_change() * 100
        trend_data['利润环比'] = trend_data['利润'].pct_change() * 100
        
        return trend_data

    def detect_anomalies(self, df: pd.DataFrame, threshold: float = 2.0) -> List[Dict[str, Any]]:
        anomalies = []
        
        if df.empty or len(df) < 10:
            return anomalies
        
        trend_data = self.analyze_trends(df, 'M')
        
        if len(trend_data) >= 3:
            sales_changes = trend_data['销售额环比'].dropna()
            profit_changes = trend_data['利润环比'].dropna()
            
            for _, row in trend_data.iterrows():
                period = row['period']
                sales_pct = row['销售额环比']
                profit_pct = row['利润环比']
                
                if pd.notna(sales_pct):
                    if abs(sales_pct) > 30:
                        anomalies.append({
                            'type': 'sales_fluctuation',
                            'period': period,
                            'metric': '销售额',
                            'change_pct': sales_pct,
                            'severity': 'high' if abs(sales_pct) > 50 else 'medium',
                            'message': f"{period} 销售额较上期{'增长' if sales_pct > 0 else '下跌'} {abs(sales_pct):.1f}%"
                        })
                
                if pd.notna(profit_pct):
                    if abs(profit_pct) > 40:
                        anomalies.append({
                            'type': 'profit_fluctuation',
                            'period': period,
                            'metric': '利润',
                            'change_pct': profit_pct,
                            'severity': 'high' if abs(profit_pct) > 60 else 'medium',
                            'message': f"{period} 利润较上期{'增长' if profit_pct > 0 else '下跌'} {abs(profit_pct):.1f}%"
                        })
        
        if '地区' in df.columns:
            region_data = df.groupby('地区').agg({
                '销售额': 'sum',
                '利润': 'sum'
            }).reset_index()
            
            avg_sales = region_data['销售额'].mean()
            for _, row in region_data.iterrows():
                if row['销售额'] < avg_sales * 0.3:
                    anomalies.append({
                        'type': 'low_performance',
                        'dimension': '地区',
                        'value': row['地区'],
                        'metric': '销售额',
                        'actual': row['销售额'],
                        'average': avg_sales,
                        'severity': 'medium',
                        'message': f"地区 '{row['地区']}' 销售额仅为平均水平的 {row['销售额']/avg_sales*100:.1f}%"
                    })
        
        return anomalies

    def analyze_by_dimension(self, df: pd.DataFrame, dimension: str) -> pd.DataFrame:
        if df.empty or dimension not in df.columns:
            return pd.DataFrame()
        
        agg_dict = {
            '销售额': 'sum',
            '利润': 'sum',
        }
        if '数量' in df.columns:
            agg_dict['数量'] = 'sum'
        
        dim_data = df.groupby(dimension).agg(agg_dict).reset_index()
        
        total_sales = dim_data['销售额'].sum()
        total_profit = dim_data['利润'].sum()
        
        dim_data['销售额占比'] = dim_data['销售额'] / total_sales * 100 if total_sales > 0 else 0
        dim_data['利润占比'] = dim_data['利润'] / total_profit * 100 if total_profit > 0 else 0
        dim_data['利润率'] = dim_data['利润'] / dim_data['销售额'] * 100
        
        dim_data = dim_data.sort_values('销售额', ascending=False)
        
        return dim_data


class DataProcessor:
    REQUIRED_COLUMNS = ['订单日期', '地区', '类别', '产品名', '销售额', '利润', '销售人员']
    
    def __init__(self):
        self.raw_data = None
        self._cleaned_data = None
        self.cleaning_stats = {}

    def load_data(self, file_path) -> Tuple[bool, str]:
        try:
            if file_path is None:
                return False, "请上传 CSV 文件"

            self.raw_data = pd.read_csv(file_path)
            return True, "数据加载成功"
        except Exception as e:
            return False, f"数据加载失败: {str(e)}"

    def validate_columns(self) -> Tuple[bool, str]:
        if self.raw_data is None:
            return False, "数据未加载"

        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in self.raw_data.columns]
        if missing_columns:
            return False, f"缺少必需列: {', '.join(missing_columns)}"

        return True, "列验证通过"

    def clean_data(self) -> Dict[str, Any]:
        if self.raw_data is None:
            return {'success': False, 'message': '数据未加载'}

        df = self.raw_data.copy()
        original_count = len(df)

        self.cleaning_stats = {
            'original_count': original_count,
            'duplicates_removed': 0,
            'null_rows_removed': 0,
            'final_count': 0
        }

        duplicates = df.duplicated()
        if duplicates.any():
            self.cleaning_stats['duplicates_removed'] = duplicates.sum()
            df = df.drop_duplicates()

        key_columns = ['订单日期', '地区', '类别', '产品名', '销售额', '利润', '销售人员']
        null_mask = df[key_columns].isnull().any(axis=1)
        if null_mask.any():
            self.cleaning_stats['null_rows_removed'] = null_mask.sum()
            df = df[~null_mask]

        df['订单日期'] = pd.to_datetime(df['订单日期'], errors='coerce')
        invalid_dates = df['订单日期'].isnull()
        if invalid_dates.any():
            self.cleaning_stats['null_rows_removed'] += invalid_dates.sum()
            df = df[~invalid_dates]

        for col in ['销售额', '利润']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            invalid_nums = df[col].isnull()
            if invalid_nums.any():
                self.cleaning_stats['null_rows_removed'] += invalid_nums.sum()
                df = df[~invalid_nums]

        df['年份'] = df['订单日期'].dt.year
        df['月份'] = df['订单日期'].dt.month
        df['年月'] = df['订单日期'].dt.to_period('M')

        self._cleaned_data = df.reset_index(drop=True)
        self.cleaning_stats['final_count'] = len(self._cleaned_data)

        return {
            'success': True,
            'message': '数据清洗完成',
            'stats': self.cleaning_stats
        }

    def filter_data(
        self,
        date_range: Tuple[datetime, datetime] = None,
        regions: list = None,
        categories: list = None,
        salespeople: list = None
    ) -> pd.DataFrame:
        if self._cleaned_data is None:
            return pd.DataFrame()

        df = self._cleaned_data.copy()

        if date_range:
            start_date, end_date = date_range
            df = df[(df['订单日期'] >= pd.Timestamp(start_date)) & (df['订单日期'] <= pd.Timestamp(end_date))]

        if regions and len(regions) > 0:
            df = df[df['地区'].isin(regions)]

        if categories and len(categories) > 0:
            df = df[df['类别'].isin(categories)]

        if salespeople and len(salespeople) > 0:
            df = df[df['销售人员'].isin(salespeople)]

        return df

    def get_unique_values(self) -> Dict[str, list]:
        if self._cleaned_data is None:
            return {}

        return {
            'regions': sorted(self._cleaned_data['地区'].unique().tolist()),
            'categories': sorted(self._cleaned_data['类别'].unique().tolist()),
            'salespeople': sorted(self._cleaned_data['销售人员'].unique().tolist()),
            'date_range': (self._cleaned_data['订单日期'].min(), self._cleaned_data['订单日期'].max())
        }

    def calculate_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty:
            return {
                'total_sales': 0,
                'total_profit': 0,
                'order_count': 0,
                'avg_order_sales': 0
            }

        return {
            'total_sales': df['销售额'].sum(),
            'total_profit': df['利润'].sum(),
            'order_count': len(df),
            'avg_order_sales': df['销售额'].mean()
        }
