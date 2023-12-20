from event_correlation_service.service import App
import click


@click.group()
def main():
    pass


@main.command()
def run():
    app = App()
    app.run()


if __name__ == '__main__':
    main()
