/// <binding BeforeBuild='beforeBuild' Clean='clean' AfterBuild='afterBuild' />
"use strict";

var gulp = require("gulp"),
    rimraf = require("rimraf"),
    minifyCss = require('gulp-minify-css'),
    concat = require('gulp-concat'),
    uglify = require('gulp-uglify');

var paths = {
    webroot: "./",
    node_modules: "./node_modules/"
};

paths.jqueryJs = paths.node_modules + "jquery/dist/jquery.min.js";
paths.bootstrapJs = paths.node_modules + "bootstrap/dist/js/bootstrap.min.js";
paths.bootstrapCss = paths.node_modules + "bootstrap/dist/css/bootstrap.min.css";
paths.concatJsDest = paths.webroot + "r.js";
paths.concatCssDest = paths.webroot + "r.css";

gulp.task("clean:js", function (cb) {
    return rimraf(paths.concatJsDest, cb);
});
gulp.task("clean:css", function (cb) {
    return rimraf(paths.concatCssDest, cb);
});
gulp.task("clean", ["clean:js", "clean:css"]);

gulp.task("beforeBuild:js", function () {
    return gulp.src([paths.jqueryJs, paths.bootstrapJs])
        .pipe(concat(paths.concatJsDest))
        .pipe(uglify())
        .pipe(gulp.dest("."));
});
gulp.task("beforeBuild:css", function () {
    return gulp.src([paths.bootstrapCss])
        .pipe(concat(paths.concatCssDest))
        .pipe(minifyCss())
        .pipe(gulp.dest("."));
});
gulp.task("beforeBuild", ["beforeBuild:js", "beforeBuild:css"]);
