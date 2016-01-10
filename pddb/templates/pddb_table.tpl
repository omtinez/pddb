%# Template to generate a HTML table from a list of lists
<table id={{dom_id}} class="table">
    <thead>
        <tr>
        %# Leave blank header for first column
        <th style="display:none"></th>
        %for col in schema:
            <th>{{col}}</th>
        %end
        </tr>
    </thead>
    <tbody>
    %for row in rows:
        <tr>
        %# First column is always the id of the record
        <td style="display:none">{{row[0]}}</td>
        %for col in row[1:]:
            <td>{{col}}</td>
        %end
        </tr>
    %end
    </tbody>
</table>