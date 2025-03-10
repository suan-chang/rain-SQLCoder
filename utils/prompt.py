from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Optional, Union


class AlpacaTemplate:
    DEFAULT_SYSTEM = (
        "Below is an instruction that describes a task. "
        "Write a response that appropriately completes the request."
    )

    @classmethod
    def template(
        cls,
        user_content: str,
        system_content: Union[str, None] = None,
        response: Union[str, None] = None,
    ) -> str:
        template: str = ""

        if system_content:
            template += f"{system_content}\n\n"
        else:
            template += f"{cls.DEFAULT_SYSTEM}\n\n"

        template += f"### Instruction:\n{user_content}\n\n### Response:\n"
        if response:
            template += response

        return template


class SftPrompt(ABC):
    @classmethod
    @abstractmethod
    def prompt_user_content(cls, data: Dict[str, Any]) -> str:
        pass

    @classmethod
    @abstractmethod
    def prompt_system_content(cls, data: Dict[str, Any]) -> Optional[str]:
        pass

    @classmethod
    @abstractmethod
    def prompt_target(cls, sql: str) -> str:
        pass

    @classmethod
    def prompt(cls, data: Dict[str, Any]) -> Tuple[str, Optional[str], str]:
        user_content = cls.prompt_user_content(data)
        system_content = cls.prompt_system_content(data)
        if "spark_sql" in data:
            target = cls.prompt_target(data["spark_sql"])
        else:
            target = None
        return user_content, system_content, target


class SQLGeneratePrompt(SftPrompt):
    @classmethod
    def split_res(
        cls, res: Union[str, List[str], None]
    ) -> Union[str, List[Union[str, None]], None]:
        if res == None:
            return res
        if isinstance(res, list):
            for i in range(len(res)):
                res[i] = cls.split_res(res[i])
            return res
        else:
            res = res.strip()
            if not (res.startswith("```sql") and res.endswith("```")):
                return None
            return res[6:-3].strip()

    @classmethod
    def compose_extra_task_desc(
        cls,
        hint: Union[str, None],
        related_question_sqls: Union[List[Dict[str, str]], None],
    ) -> str:
        if not hint and not related_question_sqls:
            return ""
        elif hint and not related_question_sqls:
            return (
                "I provide generation hint, you can refer to it to help you generate.\n"
            )
        elif not hint and related_question_sqls:
            return "I provide other user queries related to the user query with the corresponding Spark SQL queries, you can refer to them to help you generate.\n"
        else:
            return "I provide generation hint and other user queries related to the user query with the corresponding Spark SQL queries, you can refer to them to help you generate.\n"

    @classmethod
    def compose_hint_content(cls, hint: Union[str, None]) -> str:
        if hint == None:
            return ""
        if len(hint) == 0:
            return ""
        hint = hint.strip()
        content = (
            "[BEGIN OF GENERATION HINT]\n" f"{hint}\n" "[END OF GENERATION HINT]\n" "\n"
        )
        return content

    @classmethod
    def compose_related_question_sqls_content(
        cls, related_question_sqls: Union[List[Dict[str, str]], None]
    ) -> str:
        if related_question_sqls == None:
            return ""
        if len(related_question_sqls) == 0:
            return ""
        content = "[BEGIN OF RELATED QUERIES]\n"
        for i, question_sql in enumerate(related_question_sqls):
            question, sql = question_sql["question"], question_sql["spark_sql"]

            question = question.strip()
            question = question.replace("\n", " ")
            sql = sql.strip()

            sub_content = (
                f"# Related Query {i+1}\n"
                "## User Query\n"
                f"`{question}`\n"
                "## Spark SQL Query\n"
                "```sql\n"
                f"{sql}\n"
                "```\n"
                "\n"
            )
            content = content + sub_content
        content = content.strip() + "\n"
        content += "[END OF RELATED QUERIES]\n\n"
        return content

    @classmethod
    def extract_table_schema(cls, user_content: str) -> str:
        start_idx = user_content.find("[BEGIN OF TABLE SCHEMAS]") + len(
            "[BEGIN OF TABLE SCHEMAS]"
        )
        end_idx = user_content.find("[END OF TABLE SCHEMAS]")
        return user_content[start_idx:end_idx].strip()

    @classmethod
    def extract_user_query(cls, user_content: str) -> str:
        start_idx = user_content.find("[BEGIN OF QUERY]\nUser Query: ") + len(
            "[BEGIN OF QUERY]\nUser Query: "
        )
        end_idx = user_content.find("[END OF QUERY]")
        return user_content[start_idx:end_idx].strip()

    @classmethod
    def extract_hint(cls, user_content: str) -> Union[str, None]:
        start_idx = user_content.find("[BEGIN OF GENERATION HINT]") + len(
            "[BEGIN OF GENERATION HINT]"
        )
        end_idx = user_content.find("[END OF GENERATION HINT]")
        if end_idx == -1:
            return None
        return user_content[start_idx:end_idx].strip()

    @classmethod
    def extract_related_question_sqls(
        cls, user_content: str
    ) -> Union[List[Dict[str, str]], None]:
        start_idx = user_content.find("[BEGIN OF RELATED QUERIES]") + len(
            "[BEGIN OF RELATED QUERIES]"
        )
        end_idx = user_content.find("[END OF RELATED QUERIES]")
        if end_idx == -1:
            return None
        related_question_sqls = user_content[start_idx:end_idx].strip().split("\n\n")
        res = []
        for question_sql in related_question_sqls:
            question_start_idx = question_sql.find("`") + 1
            question_end_idx = question_sql.find("`\n##")
            question = question_sql[question_start_idx:question_end_idx]

            sql_start_idx = question_sql.find("```sql\n") + 7
            sql_end_idx = -3
            sql = question_sql[sql_start_idx:sql_end_idx]
            res.append({"question": question, "spark_sql": sql})
        return res

    @classmethod
    def prompt_user_content(cls, data: Dict[str, Any]) -> str:
        question, schema = data["question"].strip(), data["schema"].strip()
        hint, related_question_sqls = (
            data["hint"],
            data["related_question_sqls"],
        )
        extra_task_desc = cls.compose_extra_task_desc(hint, related_question_sqls)
        hint_content = cls.compose_hint_content(hint)
        related_question_sqls_content = cls.compose_related_question_sqls_content(
            related_question_sqls
        )

        user_content = (
            "[BEGIN OF TASK INSTRUCTION]\n"
            "You are an expert in composing Spark SQL queries. You are given a user query and a set of table schemas.\n"
            "Based on the user query, you need to generate one Spark SQL query to achieve the purpose.\n"
            f"{extra_task_desc}"
            "[END OF TASK INSTRUCTION]\n"
            "\n"
            "[BEGIN OF TABLE SCHEMAS]\n"
            f"{schema}\n"
            "[END OF TABLE SCHEMAS]\n"
            "\n"
            f"{hint_content}"
            f"{related_question_sqls_content}"
            "[BEGIN OF FORMAT INSTRUCTION]\n"
            "The output MUST strictly adhere to the following format, and NO other text MUST be included.\n"
            "```sql\n"
            "your output Spark SQL query\n"
            "```\n"
            "[END OF FORMAT INSTRUCTION]\n"
            "\n"
            "[BEGIN OF QUERY]\n"
            f"User Query: {question}\n"
            "[END OF QUERY]\n"
        )

        user_content = AlpacaTemplate.template(user_content)
        return user_content

    @classmethod
    def prompt_system_content(cls, data: Dict[str, Any]) -> Union[str, None]:
        return None

    @classmethod
    def prompt_target(cls, sql: Union[str, List[str]]) -> Union[str, List[str]]:
        def map_func(tmp_sql: str) -> str:
            tmp_sql = tmp_sql.strip()
            tmp_sql = tmp_sql.strip(";")
            tmp_sql = tmp_sql.strip()
            tmp_sql += ";"
            return "```sql\n" f"{tmp_sql}\n" "```"

        if isinstance(sql, str):
            target = map_func(sql)
        else:
            target = [map_func(_sql) for _sql in sql]

        return target
