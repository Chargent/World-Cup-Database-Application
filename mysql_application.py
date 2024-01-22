import pandas as pd
import mysql.connector as mysql
from mysql.connector import Error
from rich.console import Console
from rich.table import Table
from rich import box

import builder

psw = input('Insert your password: ')

mysql_builder.create_and_fill_database(psw)

db_name = 'WorldCupDB'

try:
    mydb = mysql.connect(host='localhost', user='root', password=psw)
    if mydb.is_connected():
        mycursor = mydb.cursor()
        mycursor.execute('USE ' + db_name)
        mydb.commit()
except Error as e:
    print("Error while connecting to MySQL", e)
    exit()

fd = open("queries.sql", "r")
queries = fd.read()
fd.close()
sqlCommands = queries.split(';')

queries = ["[bold green]1) Find at what stage did your team get out in a specific year.[/bold green]", "[bold white]2) Finals winning percentage of all the teams that ever played one.[/bold white]", "[bold green]3) Head-to-head matches score between two teams.[/bold green]", "[bold white]4) Top 10 referees with the most cards given.[/bold white]"]

def gonext():
    table = Table(show_header=True, header_style="bold bright_yellow", row_styles=["white", "green"], box=box.MINIMAL, border_style="white", show_footer=True)
    table.add_row("[bold green]1) Run another query[/bold green]")
    table.add_row("[bold white]0) Exit[/bold white]")
    console.print(table)
    next = console.input('[bold white]Your choice: [/bold white]')
    if next == '0':
        console.print('\n[bold bright_yellow]Bye, thanks for your time![bold bright_yellow]\n')
        return False
    elif next == '1':
        return True
    else:
        console.print('[bold_red]Not recognized answer![/bold_red]')
        return gonext()

console=Console()
console.print("\n[bold green]Database correctly filled![/bold green]\n", style="bold blue")

console.print('''[bold bright_yellow]╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║__        __         _     _    ____                 _                ║
║\\ \\      / /__  _ __| | __| |  / ___|   _ _ __      / \\   _ __  _ __  ║
║ \\ \\ /\\ / / _ \\| '__| |/ _` | | |  | | | | '_ \\    / _ \\ | '_ \\| '_ \\ ║
║  \\ V  V / (_) | |  | | (_| | | |__| |_| | |_) |  / ___ \\| |_) | |_) |║
║   \\_/\\_/ \\___/|_|  |_|\__,_|  \\____\\__,_| .__/  /_/   \\_\\ .__/| .__/ ║
║                                         |_|             |_|   |_|    ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝[/bold bright_yellow]''')


console.print("\n[bold green]Welcome[/bold green] [bold white]to[/bold white] [bold green]the[/bold green] [bold white]World[/bold white] [bold bright_yellow]Cup[/bold bright_yellow] [bold green]Database[/bold green][bold white]![/bold white]")

counter = True
while counter:
    counter2 = True
    while counter2:
        console = Console()
        table = Table(show_header=True, header_style="bold bright_yellow", row_styles=["white", "green"], box=box.MINIMAL, border_style="white", show_footer=True)
        table.add_column("Choose a query to run:")
        for query in queries:
            table.add_row(query)
        table.add_row("[bold green]0) Exit[/bold green]")
        console.print(table)
        choice = console.input("[bold white]Your choice: [/bold white]")
        try:
            query_n = int(choice)
            counter2 = False
            if query_n >= 0:
                query_n -= 1
        except:
            print('\nNot recognized answer!')
        if query_n > 5:
            print('\nNot recognized answer!')
    
    if query_n == 0:
        team=console.input('[bold green]Insert team: [/bold green]')
        year=console.input('[bold white]Insert year: [/bold white]')
        mycursor.execute(sqlCommands[0],(year,team,year,team,team))
        result = mycursor.fetchall()
        if not result[0][0] or result[0][0] == None:
            console.print('[bold red]'+'\nEither the inputed name or year is wrong!'+'[/bold red]')
            counter = True
            counter2 = False
        else:
            if result[0][0] == 'World Cup Winner':
                console.print("\n[bold green]" + team.capitalize() + ' [bold bright_yellow]won[/bold bright_yellow] the ' + year + ' World cup!'+"[/bold green]")
                counter = gonext()
            else:
                console.print("\n[bold green]" + team.capitalize() + ' [bold red]was out[/bold red] of the ' + year + ' World Cup in the ' + "[bold bright_yellow]" + result[0][0] + "[/bold bright_yellow]" + '!'+"[/bold green]")
                counter = gonext()
    
    if query_n == 1:
        mycursor.execute(sqlCommands[1])
        result = mycursor.fetchall()
        if not result:
            console.print('[bold red]'+'No results!'+'[/bold red]')
            counter = True
            counter2 = False
        else:
            query2_df = pd.DataFrame(result, columns=['Teams', 'WinningPercentage', 'FinalsAppearances'])
            console=Console()
            table = Table(show_header=True, header_style="bold bright_yellow", row_styles=["white", "green"])
            for column in query2_df.columns:
                table.add_column(column)
            for row in query2_df.itertuples(index=False):
                table.add_row(*[str(getattr(row, col)) for col in query2_df.columns])
            print('\n')
            console.print(table)
            counter = gonext()

    if query_n == 2:
        team1=console.input('[bold green]Insert team 1: [/bold green]')
        team2=console.input('[bold white]Insert team 2: [/bold white]')
        mycursor.execute(sqlCommands[2],(team1,team2,team2,team1))
        result = mycursor.fetchall()
        if not result:
            console.print('[bold red]'+'\nThis teams never played against each other!'+'[/bold red]')
            counter = True
            counter2 = False
        else:
            query3_df = pd.DataFrame(result, columns=['Outcome','NumberOfMatches'])
            console=Console()
            table = Table(show_header=True, header_style="bold bright_yellow", row_styles=["white", "green"])
            for column in query3_df.columns:
                table.add_column(column)
            for row in query3_df.itertuples(index=False):
                table.add_row(*[str(getattr(row, col)) for col in query3_df.columns])
            print('\n')
            console.print(table)
            counter = gonext()

    if query_n == 3:
        mycursor.execute(sqlCommands[3])
        result = mycursor.fetchall()
        if not result:
            console.print('[bold red]'+'No results!'+'[/bold red]')
            counter = True
            counter2 = False
        else:
            query4_df = pd.DataFrame(result, columns=['Referee','TotalCards'])
            console=Console()
            table = Table(show_header=True, header_style="bold bright_yellow", row_styles=["white", "green"])
            for column in query4_df.columns:
                table.add_column(column)
            for row in query4_df.itertuples(index=False):
                table.add_row(*[str(getattr(row, col)) for col in query4_df.columns])
            print('\n')
            console.print(table)
            counter = gonext()

    if query_n == -1:
        console.print('\n[bold bright_yellow]Bye, thanks for your time![bold bright_yellow]\n')
        counter = False
