function onEdit(e) {
    var sheet = e.source.getActiveSheet();
    var range = e.range;
    
    var payload = {
      'sheetName': sheet.getName(),
      'updates': []
    };
  
    for (var i = 0; i < range.getNumRows(); i++) {
      for (var j = 0; j < range.getNumColumns(); j++) {
        var row = range.getRow() + i;
        var col = range.getColumn() + j;
        var value = sheet.getRange(row, col).getValue();
        
        payload.updates.push({
          'row': row,
          'col': col,
          'value': value
        });
      }
    }
  
    var options = {
      'method': 'post',
      'contentType': 'application/json',
      'payload': JSON.stringify(payload)
    };
  
    UrlFetchApp.fetch('http://127:0:0:1:5000/api/update_mysql', options);
  }