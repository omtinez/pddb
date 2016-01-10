var PandasDatabase = PandasDatabase || {}

PandasDatabase.loadTable = function loadTable(tname, targetElem, targetTitle, columns, callback) {
    reqQuery = {"_": $.now()};
    if (columns && typeof (columns) == 'string') {
        reqQuery['columns'] = columns;
    } else if (columns && typeof (columns) == 'object'
        && Object.prototype.toString.call(columns) == '[object Array]') {
        reqQuery['columns'] = columns.join(',');
    }

    // Add the title to the title element
    if (targetTitle) {
        $(targetTitle).text('Table ' + tname);
    }

    // Request the table data from the database
    $.get('/pddb/html/' + tname, reqQuery, function (data) {
        $(targetElem).empty().append(data);
        var table = $(targetElem).find('table').DataTable();
        table.column(0).visible(false);

        if (callback && typeof (callback) == 'function') {
            callback(table);
        }

    });
};

PandasDatabase.loadDb = function loadDb(targetElem, targetTitle, callback) {
    $(targetTitle).text('Database Explorer');
    $.get('/pddb/html/tables', function (data) {
        $(targetElem).html(data);
        var table = $(targetElem).find('table').DataTable();
        $(targetElem).find('table tbody tr td').each(function () {
            $(this).css('cursor', 'pointer');
            $(this).click(function () {
                PandasDatabase.loadTable($(this).text(), targetElem, targetTitle, null, null, callback);
            });
        });

        if (callback && typeof (callback) == 'function') {
            callback(table);
        }
    });
}
