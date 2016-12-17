# all the imports
import os
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash

#######################################################################
# Based on tutorial
app = Flask(__name__)
app.config.from_object(__name__)

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'flaskr.db'),
    SECRET_KEY='development key',
    USERNAME='admin',
    PASSWORD='default'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)
def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()

def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv

def init_db():
    db = get_db()
    with app.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()

@app.cli.command('initdb')
def initdb_command():
    """Initializes the database."""
    with app.app_context():
        init_db()    
    print('Initialized the database.')

#######################################################################

@app.route('/')
def show_entries():
    if 'beingshown' not in session:
        session['beingshown'] = 'all'
    db = get_db()
    #u changed this and show_entries and suddenly its not working. rip
    cur = db.execute('select title, text from entries order by id desc')
    entries = cur.fetchall()
    return render_template('show_entries.html', entries=entries)

@app.route('/earnings')
def earnings():
    session['beingshown'] = 'earnings'
    return redirect(url_for('show_entries'))

@app.route('/crimes')
def crimes():
    session['beingshown'] = 'crimes'
    return redirect(url_for('show_entries'))

@app.route('/buildings')
def buildings():
    session['beingshown'] = 'buildings'
    return redirect(url_for('show_entries'))

@app.route('/allinfo')
def allinfo():
    session['beingshown'] = 'all'
    return redirect(url_for('show_entries'))