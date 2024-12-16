import dash
from dash import Dash, html, dcc, callback, Output, Input
from dotenv import load_dotenv
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

load_dotenv("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/configs/prod/prod.env")

app = Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)

nav_items = [
        dbc.NavItem(dbc.NavLink(f"{page['name']}", href=f"{page['relative_path']}"))
        for page in dash.page_registry.values()
]

dropdown_components = [
    dbc.DropdownMenu(
    children=[
        html.Div([
            dbc.Input(
                id="env_variable_path", 
                placeholder="Input Path to environment variables", 
                type="text", persistence=True, debounce=True
            ),
        ])
        
    ],
    nav=True,
    in_navbar=True,
    label="More"
    ),
]

app.layout = html.Div([
    dbc.NavbarSimple(
    children=nav_items + dropdown_components,
    brand="NavbarSimple",
    brand_href="#",
    color="primary",
    dark=True
    ),

    dash.page_container
])

@dash.callback(
    Input("env_variable_path", "value")
)
def load_environment_variables(value: str):
    if value is not None:
        print(f"Loading environment from {value}")
        load_dotenv(value)
    else:
        raise PreventUpdate

if __name__ == '__main__':
    app.run(debug=True)
