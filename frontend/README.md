# 📅 Real Madrid Castilla - Calendario Guatemala

Frontend web para el calendario automático del Real Madrid Castilla con horarios de Guatemala.

## 🌟 Características

- ⚽ **Calendario automático** con datos reales
- 📱 **Compatible con iOS** - suscripción directa
- 🇬🇹 **Horarios Guatemala** (GMT-6)
- 🔄 **Actualizaciones en tiempo real**
- 📊 **Dashboard de estado** del sistema
- 🎨 **Diseño responsive** y moderno

## 🚀 Tecnologías

- **HTML5** semántico
- **CSS3** con variables personalizadas
- **JavaScript** vanilla (sin frameworks)
- **Responsive design** mobile-first
- **PWA ready** (Service Worker preparado)

## 📱 URLs del Proyecto

- **Frontend**: https://calendario-castilla.vercel.app
- **Backend API**: https://calendario-castilla.onrender.com
- **Calendario iOS**: https://calendario-castilla.onrender.com/calendar.ics

## 🔧 Desarrollo Local

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/calendario-castilla-frontend.git
cd calendario-castilla-frontend

# Abrir con servidor local
python -m http.server 8000
# O con Node.js
npx serve .

# Abrir en navegador
open http://localhost:8000
```

## 🎯 Configuración

En `index.html`, línea ~564, cambiar la URL del backend:

```javascript
const CONFIG = {
    API_BASE: 'https://tu-backend.onrender.com', // 🚨 CAMBIAR AQUÍ
    // ... resto de configuración
};
```

## 📦 Deploy en Vercel

1. **Push a GitHub**
2. **Conectar con Vercel**
3. **Deploy automático**

## 🤝 Contribuciones

¡Las contribuciones son bienvenidas!

1. Fork del proyecto
2. Crear feature branch
3. Commit de cambios
4. Push al branch
5. Abrir Pull Request

## 📄 Licencia

Este proyecto es de código abierto para la comunidad madridista.

## 👑 ¡Hala Madrid!

Creado con ❤️ por un fan guatemalteco del Real Madrid.