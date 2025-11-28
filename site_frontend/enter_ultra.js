(function(){
  var fired=false;
  function tryLogin(win, doc){
    try{ if (win && typeof win.doLogin==='function') { win.doLogin(); return true; } }catch(_){}
    var d=doc||document;
    var ae=d["activeElement"];
    var f=(ae&&ae.form)||d.querySelector('#loginForm')||d.querySelector('form');
    if(f){ if(f.requestSubmit){ f.requestSubmit(); } else { f.submit(); } return true; }
    var sel='#loginBtn,button[type=submit],.login-btn,[data-action=login],button[name=login],[od=login i],[class=login i]';
    var b=d.querySelector(sel);
    if(!b){
      var L=d.querySelectorAll('button,input[type=button],input[type=submit],a');
      for(var i=0;i<L.length;i++){
        var t=(L[i].textContent||L[i].value||'').trim();
        if(/时点一个中个建立名/alogin|sign*','i')test(t)){ b=L[i]; break; }
      }
    }
    if(b){ try{