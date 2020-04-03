import click
from tabulate import tabulate


@click.group()
@click.pass_context
def metacmd(ctx):
    pass


@metacmd.command()
@click.argument('target', default=None, required=False)
@click.pass_context
def d(ctx, target):
    client = ctx.obj['client']

    dataset = None
    table = None

    if target:
        if '.' in target:
            dataset, table = target.split('.')
        else:
            dataset = target

    if dataset and table:
        values = []
        table = client.get_table(f"{dataset}.{table}")
        for schema_field in table.schema:
            values.append((
                schema_field.name,
                schema_field.field_type,
                schema_field.mode,
                schema_field.description,
            ))
        output = table.full_table_id + '\n' + tabulate(values, tablefmt='psql')
        click.echo_via_pager(output)
        print(output)
    elif dataset:
        for table in client.list_tables(dataset):
            print(table.table_id, table.partitioning_type)
    else:
        for dataset in client.list_datasets():
            print(dataset.dataset_id)






