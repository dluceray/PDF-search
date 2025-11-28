(function(){function h(e){if(e.isComposing||e.key!=="Enter")return;e.preventDefault();var d=document;
try{var p=d.querySelector("input[type=password],#pwd");var v=p?(p.value||"").trim():"";if(typeof doLogin==="function"){doLogin(v);return}}catch(_){ }
var f=d.forms[0];if(f){if(f.requestSubmit)f.requestSubmit();else f.submit();return}
var b=d.querySelector("#btnLogin,#loginBtn,button[type=submit],.login-btn");if(!b){var L=d.querySelectorAll("button,input[type=submit]");for(var i=0;i<L.length;i++){var t=(L[i].textContent||L[i].value||"").trim();if(/登录|login/i.test(t)){b=L[i];break}}}
if(b){try{b.disabled=false}catch(_){ }b.click();}}document.addEventListener("keydown",h,true);})();
