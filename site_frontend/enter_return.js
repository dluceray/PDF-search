(function(){
  var fired=false;
  function q(s){return document.querySelector(s);}
  function pickBtn(){
    var b = document.querySelector('#loginBtn, button[type="submit"], .login-btn, [data-action="login"], button[name="login"]');
    if (b) return b;
    var list = document.querySelectorAll('button, input[type="button"], input[type="submit"]');
    for (var i=0;i<list.length;i++){
      var t=(list[i].textContent||list[i].value||'').trim();
      if (/登录|login/i.test(t)) return list[i];
    }
    return null;
  }
  function trigger(){
    try{ if (typeof window.doLogin==='function'){ window.doLogin(); return; } }catch(_){}
    var pwd  = document.querySelector('input[type="password"], #password');
    var form = (pwd && pwd.form) || document.querySelector('#loginForm') || document.querySelector('form');
    if (form){ if (form.requestSubmit){ form.requestSubmit(); } else { form.submit(); } return; }
    var btn = pickBtn(); if (btn){ try{btn.disabled=false;}catch(_){;} btn.click(); }
  }
  function onKey(e){
    if (e.isComposing) return;
    if (e.key==='Enter'){
      e.preventDefault();
      if (fired) return; fired=true; setTimeout(function(){fired=false;},800);
      trigger();
    }
  }
  document.addEventListener('keydown', onKey, true);
})();