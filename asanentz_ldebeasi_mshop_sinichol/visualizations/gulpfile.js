var gulp = require('gulp');
var concat = require('gulp-concat');
var sass = require('gulp-sass');
var cleanCSS = require('gulp-clean-css');
var rename = require('gulp-rename');
var stripDebug = require('gulp-strip-debug');
var uglify = require('gulp-uglify');
var order = require('gulp-order');

var paths = {
  sass: ['./assets/scss/**/*.scss'],
  js: ['./assets/js/*.js']
};

gulp.task('default', ['watch']);

gulp.task('styles', function(done) {
  gulp.src('./assets/scss/*')
  .pipe(order([
      "bootstrap.min.scss",
      "bootstrap-theme.min.css",
      "animations.scss",
      "fonts.scss",
      "app.scss"
  ]))
    .pipe(sass({
      errLogToConsole: true
    }))
    .pipe(cleanCSS({
      compatibility: 'ie8'
    }))
    .pipe(concat("app.min.css"))
    .pipe(gulp.dest('./assets/css/'))
    .on('end', done);
    console.log("   *** compiled app.min.css ***   ")
});

// Combine, minify, and clean JS files -- orders js files
gulp.task('scripts', function() {  
    gulp
  .src("./assets/js/*.js")
  .pipe(order([
      "jquery.js",
      "fastclick.js",
      "app.js"
  ]))
  //.pipe(stripDebug())
  //.pipe(uglify())
  .pipe(concat("app.min.js"))
  .pipe(gulp.dest("./assets/js/min/"));
        console.log('   *** compiled app.min.js ***   ');
});

// Watch SCSS and JS
gulp.task('watch', function() {
    gulp.watch(paths.sass, ['styles']);
    gulp.watch(paths.js, ['scripts']);
});


