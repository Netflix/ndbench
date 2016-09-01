var browserSync = require('browser-sync').create();
var modRewrite = require('connect-modrewrite');

browserSync.init({
    files: 'app',
    port: 8081,
    logPrefix: 'NDBench UI - DEV',
    server: {
        baseDir: 'app',
        middleware: [
            modRewrite([
                '^/REST/(.*)$ http://localhost:8080/REST/$1 [P]',
            ])
        ]
    }
});
