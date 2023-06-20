import boto3
import gokart
import luigi
from gokart.target import TargetOnKart
from luigi.target import os
from luigi.task import flatten


class TaskA(gokart.TaskOnKart):
    param = luigi.Parameter()

    def output(self):
        return self.make_target("output_of_task_a.pkl")

    def run(self):
        results = f"param={self.param}"
        self.dump(results)


class TaskB(gokart.TaskOnKart):
    param = luigi.Parameter()

    def requires(self):
        return TaskA(param="param_for_a")

    def output(self):
        return self.make_target("output_of_task_b.pkl")

    def run(self):
        input_data = self.load()  # Load the output of TaskA
        results = f"Task B input: {input_data}, param={self.param}"
        self.dump(results)


class TaskC(gokart.TaskOnKart):
    param = luigi.Parameter()

    def requires(self):
        return dict(a=TaskA(param="param_for_other_a"), b=TaskB(param="param_for_b"))

    def output(self):
        # output of TaskC depends on the output of TaskA and TaskB
        return self.input()

    def run(self):
        output_of_task_a = self.load("a")
        output_of_task_b = self.load("b")
        results = f"Task A: {output_of_task_a}\nTask B: {output_of_task_b}\nTask C: param={self.param}"
        print(results)


class TaskD(gokart.TaskOnKart):
    param = luigi.Parameter()

    def requires(self):
        return dict(c=TaskC(param="param_for_c"))

    def output(self):
        return self.make_target("output_of_task_d.pkl")

    def run(self):
        output_of_task_c = self.load("c")
        results = f"Task C: {output_of_task_c}\nTask D: param={self.param}"
        self.dump(results)


def _retrieve_dependent_task(root_task: gokart.TaskOnKart):
    """
    Recursively retrieve all tasks that are dependent on the root_task, including the root_task.
    """
    dependent_tasks = []

    def _retrieve_children_task(task: gokart.TaskOnKart) -> None:
        nonlocal dependent_tasks
        if task in dependent_tasks:
            return
        child_tasks = flatten(task.requires())
        for child_task in child_tasks:
            _retrieve_children_task(child_task)
        dependent_tasks.append(task)

    _retrieve_children_task(root_task)
    return dependent_tasks


def _delete_cache(task: gokart.TaskOnKart):
    if not isinstance(task, gokart.TaskOnKart):
        return
    if not isinstance(task.output(), TargetOnKart):
        return

    file_path = task.output().path()
    if file_path.startswith("s3://"):
        s3 = boto3.resource("s3")
        bucket_name, key = file_path[5:].split("/", 1)
        s3.Object(bucket_name, key).delete()
        print("Deleted: ", file_path)
    elif os.path.exists(file_path):
        os.remove(file_path)
        print("Deleted: ", file_path)


def delete_cache(task: gokart.TaskOnKart):
    tasks = _retrieve_dependent_task(task)
    print(tasks)
    for t in tasks:
        _delete_cache(t)


if __name__ == "__main__":
    luigi.configuration.LuigiConfigParser.add_config_path("../param.ini")
    task = TaskD(param="param_for_d")
    gokart.build(task)
    delete_cache(task)
