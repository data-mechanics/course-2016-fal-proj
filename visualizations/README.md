# Sandbox Template
A lightweight template for creating responsive websites

## Installation

* Clone this repository to your local machine using
``` 
$ git clone https://github.com/liamdebeasi/sandbox-template.git
```
* On your local machine, run in the sandbox-template directory
```
$ npm install
```
_Note: macOS users may need to run `sudo npm install`_


## Usage

Sandbox Template uses a Gulp task to watch your CSS and JavaScript and compile when necessary.

* To watch CSS and JavaScript for changes, run
```
$ gulp watch
```

## Included
* jQuery 2.2.4
* Bootstrap Core and Bootstrap Theme 3.3.7
* FastClick 1.0.6 (removes touch delay on mobile devices)


## Version History

### 1.0.1
* Tidy up index.html
* Use gulp-clean-css instead of gulp-minify-css
* Added bootstrap, fonts, and animations css files to gulp watch

### 1.0.0
* Initial Release
* Bootstrap v3.3.7, jQuery v2.2.4
