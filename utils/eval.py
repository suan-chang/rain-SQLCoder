import re
import pandas as pd
from func_timeout import func_timeout, FunctionTimedOut
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import collections


LIKE_PATTERN = r"LIKE[\s\S]*'"


def deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns.tolist()
    if len(cols) != len(set(cols)):
        duplicates = [
            item for item, count in collections.Counter(cols).items() if count > 1
        ]
        for dup in duplicates:
            indices = [i for i, x in enumerate(cols) if x == dup]
            for i in indices:
                cols[i] = f"{dup}_{i}"
        df.columns = cols
    return df


def serializate_columns(df: pd.DataFrame):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, pd.Series))).any():
            df[col] = df[col].apply(
                lambda x: str(sorted(x)) if isinstance(x, (list, pd.Series)) else x
            )
    return df


def normalize_table(
    df: pd.DataFrame, query_category: str, if_order: bool, sql: str = None
) -> pd.DataFrame:
    """
    Normalizes a dataframe by:
    1. removing all duplicate rows
    2. sorting columns in alphabetical order
    3. sorting rows using values from first column to last (if query_category is not 'order_by' and question does not ask for ordering)
    4. resetting index
    """
    df = serializate_columns(df)

    # remove duplicate rows, if any
    df = df.drop_duplicates()

    # sort columns in alphabetical order of column names
    df = deduplicate_columns(df)
    sorted_df = df.reindex(sorted(df.columns), axis=1)

    # check if query_category is 'order_by' and if question asks for ordering
    has_order_by = False

    if query_category == "order_by" or if_order:
        has_order_by = True

        if sql:
            # determine which columns are in the ORDER BY clause of the sql generated, using regex
            pattern = re.compile(r"ORDER BY[\s\S]*", re.IGNORECASE)
            order_by_clause = re.search(pattern, sql)
            if order_by_clause:
                order_by_clause = order_by_clause.group(0)
                # get all columns in the ORDER BY clause, by looking at the text between ORDER BY and the next semicolon, comma, or parantheses
                pattern = re.compile(r"(?<=ORDER BY)(.*?)(?=;|,|\)|$)", re.IGNORECASE)
                order_by_columns = re.findall(pattern, order_by_clause)
                order_by_columns = (
                    order_by_columns[0].split() if order_by_columns else []
                )
                order_by_columns = [
                    col.strip().rsplit(".", 1)[-1] for col in order_by_columns
                ]

                ascending = False
                # if there is a DESC or ASC in the ORDER BY clause, set the ascending to that
                if "DESC" in [i.upper() for i in order_by_columns]:
                    ascending = False
                elif "ASC" in [i.upper() for i in order_by_columns]:
                    ascending = True

                # remove whitespace, commas, and parantheses
                order_by_columns = [col.strip() for col in order_by_columns]
                order_by_columns = [
                    col.replace(",", "").replace("(", "") for col in order_by_columns
                ]
                order_by_columns = [
                    i
                    for i in order_by_columns
                    if i.lower()
                    not in ["desc", "asc", "nulls", "last", "first", "limit"]
                ]

                # get all columns in sorted_df that are not in order_by_columns
                other_columns = [
                    i for i in sorted_df.columns.tolist() if i not in order_by_columns
                ]

                # only choose order_by_columns that are in sorted_df
                order_by_columns = [
                    i for i in order_by_columns if i in sorted_df.columns.tolist()
                ]
                sorted_df = sorted_df.sort_values(
                    by=order_by_columns + other_columns, ascending=ascending
                )

                sorted_df = sorted_df[other_columns + order_by_columns]

    if not has_order_by:
        # sort rows using values from first column to last
        sorted_df = sorted_df.sort_values(by=list(sorted_df.columns))

    # reset index
    sorted_df = deduplicate_columns(sorted_df)
    sorted_df = sorted_df.reset_index(drop=True)
    return sorted_df


def compare_df(
    df_gold: pd.DataFrame,
    df_gen: pd.DataFrame,
    query_category: str,
    question: str,
    query_gold: str = None,
    query_gen: str = None,
) -> bool:
    """
    Compares two dataframes and returns True if they are the same, else False.
    query_gold and query_gen are the original queries that generated the respective dataframes.
    """
    # drop duplicates to ensure equivalence
    if df_gen.empty or df_gold.empty:
        return False
    try:
        is_equal = df_gold.values == df_gen.values
        if is_equal.all():
            return True
    except:
        try:
            is_equal = df_gold.values == df_gen.values
            if is_equal:
                return True
        except:
            pass

    pattern = re.compile(r"ORDER BY[\s\S]*", re.IGNORECASE)
    is_order = re.search(pattern, query_gold)

    df_gold = normalize_table(df_gold, query_category, is_order, query_gold)
    df_gen = normalize_table(df_gen, query_category, is_order, query_gen)

    # perform same checks again for normalized tables
    if df_gold.shape != df_gen.shape:
        return False
    # fill NaNs with -99999 to handle NaNs in the dataframes for comparison
    df_gen.fillna(-99999, inplace=True)
    df_gold.fillna(-99999, inplace=True)
    is_equal = df_gold.values == df_gen.values

    try:
        return is_equal.all()
    except:
        return is_equal


def subset_df(
    df_sub: pd.DataFrame,
    df_super: pd.DataFrame,
    query_category: str,
    question: str,
    query_super: str = None,
    query_sub: str = None,
    verbose: bool = False,
) -> bool:
    """
    Checks if df_sub is a subset of df_super.
    """
    if df_sub.empty and df_super.empty:
        return True  # handle cases for empty dataframes

    if df_sub.empty:
        return False

    is_order = False
    if query_sub:
        pattern = re.compile(r"ORDER BY[\s\S]*", re.IGNORECASE)
        is_order = re.search(pattern, query_sub)

    # make a copy of df_super so we don't modify the original while keeping track of matches
    df_super_temp = df_super.copy(deep=True)
    matched_columns = []
    df_sub = deduplicate_columns(df_sub)
    df_super_temp = deduplicate_columns(df_super_temp)
    for col_sub_name in df_sub.columns:
        col_match = False
        for col_super_name in df_super_temp.columns:
            col_sub = df_sub[col_sub_name].sort_values().reset_index(drop=True)
            col_super = (
                df_super_temp[col_super_name].sort_values().reset_index(drop=True)
            )

            try:
                assert_series_equal(
                    col_sub, col_super, check_dtype=False, check_names=False
                )
                col_match = True
                matched_columns.append(col_super_name)
                # remove col_super_name to prevent us from matching it again
                df_super_temp = df_super_temp.drop(columns=[col_super_name])
                break
            except AssertionError:
                continue

        if not col_match:
            if verbose:
                print(f"no match for {col_sub_name}")
            return False

    df_sub_normalized = normalize_table(df_sub, query_category, is_order, query_sub)

    # get matched columns from df_super, and rename them with columns from df_sub, then normalize
    df_super_matched = df_super[matched_columns].rename(
        columns=dict(zip(matched_columns, df_sub.columns))
    )
    df_super_matched = normalize_table(
        df_super_matched, query_category, is_order, query_super
    )

    try:
        assert_frame_equal(df_sub_normalized, df_super_matched, check_dtype=False)
        return True
    except AssertionError:
        return False


def _check_df(
    gt_df: pd.DataFrame, pre_df: pd.DataFrame, gt_sql: str, pre_sql: str
) -> bool:
    try:
        if gt_df.empty or pre_df.empty:
            return False
        result = compare_df(gt_df, pre_df, "", "", gt_sql, pre_sql)
        if result:
            return True
        result = subset_df(gt_df, pre_df, "", "", query_sub=gt_sql, query_super=pre_sql)
        return result
    except Exception as e:
        return False


def check_df(
    gt_df: pd.DataFrame, pre_df: pd.DataFrame, gt_sql: str, pre_sql: str
) -> bool:
    try:
        res = func_timeout(10, _check_df, args=(gt_df, pre_df, gt_sql, pre_sql))
        return res
    except FunctionTimedOut as e:
        return False
    except Exception as e:
        return False
