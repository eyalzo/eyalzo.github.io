// mmm_shared.js - פונקציות והגדרות משותפות לדשבורד ולציר הזמן
const MMM_SHARED_VERSION = "1.0.1";
console.log(`[Gilat AI - Shared] mmm_shared.js loaded successfully. Version: ${MMM_SHARED_VERSION}`);

// 1. הגדרת ה-CSS המשותף
const sharedStyles = `
    .card-type-badge { color: white; padding: 2px 10px; border-radius: 6px; font-size: 0.85rem; font-weight: bold; display: inline-block; }
    #docHoverTooltip {
        position: fixed; background: rgba(15, 23, 42, 0.95); color: #fff;
        padding: 16px; border-radius: 8px; pointer-events: none; opacity: 0;
        transition: opacity 0.2s; z-index: 9999; direction: rtl;
        max-width: 450px; min-width: 320px; line-height: 1.5;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.2), 0 10px 15px -3px rgba(0,0,0,0.1);
    }
    #docHoverTooltip .tt-title { font-weight: bold; color: #93c5fd; font-size: 1.15rem; line-height: 1.3; }
    #docHoverTooltip .tt-date { font-size: 0.9rem; color: #cbd5e1; background-color: #1e293b; padding: 2px 8px; border-radius: 4px; border: 1px solid #334155; }
`;

// 2. הזרקת ה-CSS לתוך ה-HTML
document.head.insertAdjacentHTML("beforeend", `<style id="mmm-shared-styles">${sharedStyles}</style>`);

// 3. הזרקת רכיב הפופאפ ל-DOM בצורה בטוחה
function injectTooltip() {
    if (document.body && !document.getElementById('docHoverTooltip')) {
        document.body.insertAdjacentHTML("beforeend", '<div id="docHoverTooltip"></div>');
    }
}
if (document.body) { injectTooltip(); } 
else { window.addEventListener('DOMContentLoaded', injectTooltip); }

// 4. הגדרות ומשתנים גלובליים
const ENTITIES = [
    { key: "mmm", label: "מסמכי ממ״מ", color: "#8b5cf6" },
    { key: "committee", label: "דיוני ועדות", color: "#ef4444" },
    { key: "activeLaw", label: "הצעות חוק פעילות", color: "#3b82f6" },
    { key: "passedLaw", label: "חקיקה שאושרה", color: "#10b981" },
    { key: "comptroller", label: "מבקר המדינה", color: "#f59e0b" },
    { key: "govDecision", label: "החלטות ממשלה", color: "#64748b" }
];

const entityMap = {};
ENTITIES.forEach(e => entityMap[e.key] = e);

// 5. פונקציות תאריכים
function parseFlexibleDate(dateStr) {
    if (!dateStr) return { day: null, month: 1, year: 1970, sortValue: 0, hasDate: false };
    const normalized = String(dateStr).replace(/[\\\-]/g, '/');
    const parts = normalized.split('/');
    let day = null, month = 1, year = 1970;

    if (parts.length === 2) {
        if (parts[0].length === 4) { year = parseInt(parts[0]); month = parseInt(parts[1]); }
        else { month = parseInt(parts[0]); year = parseInt(parts[1]); }
    } else if (parts.length === 3) {
        if (parts[0].length === 4) { year = parseInt(parts[0]); month = parseInt(parts[1]); day = parseInt(parts[2]); }
        else { day = parseInt(parts[0]); month = parseInt(parts[1]); year = parseInt(parts[2]); }
    } else if (parts.length === 1 && parts[0].length === 4) {
        year = parseInt(parts[0]);
    }
    const sortValue = year + (month / 12) + (day ? (day / 365) : 0);
    return { day, month, year, sortValue, hasDate: true };
}

function formatParsedDate(parsed) {
    if (!parsed || !parsed.hasDate) return '';
    const mm = String(parsed.month).padStart(2, '0');
    if (parsed.day) {
        const dd = String(parsed.day).padStart(2, '0');
        return `${dd}/${mm}/${parsed.year}`;
    }
    return `${mm}/${parsed.year}`;
}

// 6. פונקציות רינדור חוקים (סטטוס)
function getLawStageIndex(statusText) {
    if (!statusText) return 0;
    const text = statusText;
    if (text.includes('הכנה לקריאה שנייה') || text.includes('הכנה לקריאה שניה')) return 3;
    if (text.includes('שנייה ושלישית') || text.includes('שניה ושלישית') || text.includes('התקבל')) return 4;
    if (text.includes('פורסם') || text.includes('סופי')) return 5;
    if (text.includes('קריאה ראשונה')) {
        if (text.includes('הכנה')) return 1;
        return 2;
    }
    return 0;
}

function generateLawProgressHtml(statusText) {
    if (!statusText) return '<span class="text-gray-500">לא צוין</span>';
    const stageIndex = getLawStageIndex(statusText);
    const pastColors = ['#96bba3', '#7ea886', '#649474', '#4b7a5a', '#326040'];
    const currentColor = '#ff9f11';
    const futureColor = '#dcdcdc';
    let dotsHtml = '';
    for (let i = 0; i < 6; i++) {
        if (i < stageIndex) {
            const color = pastColors[Math.min(i, pastColors.length - 1)];
            dotsHtml += `<li style="background-color: ${color}; width: 14px; height: 14px; border-radius: 50%; display: inline-block;"></li>`;
        } else if (i === stageIndex) {
            dotsHtml += `<li style="background-color: ${currentColor}; width: 14px; height: 14px; border-radius: 50%; display: inline-block; box-shadow: 0 0 0 2px rgba(255, 159, 17, 0.3);"></li>`;
        } else {
            dotsHtml += `<li style="background-color: ${futureColor}; width: 14px; height: 14px; border-radius: 50%; display: inline-block;"></li>`;
        }
    }
    return `
        <div style="display: flex; flex-direction: column; gap: 6px; margin-top: 4px;">
            <div style="font-weight: 600; color: #1e293b;">${statusText}</div>
            <ul style="display: flex; gap: 6px; list-style: none; padding: 0; margin: 0; align-items: center;">
                ${dotsHtml}
            </ul>
        </div>
    `;
}

// 7. פונקציות הפופאפ
function showDocTooltip(event, idCodesStr) {
    const tooltip = document.getElementById('docHoverTooltip');
    if (!tooltip) return;
    const idCodes = idCodesStr.split(',');
    let htmlContent = '';

    idCodes.forEach((idCode, index) => {
        const doc = window.appConfiguration.items.find(d => d.idCode === idCode.trim());
        if (!doc) return;

        let dateBadgeHtml = '';
        if (doc.parsedDate && doc.parsedDate.hasDate) {
            const formattedMonth = String(doc.month).padStart(2, '0');
            const day = doc.day ? String(doc.day).padStart(2, '0') + '/' : '';
            dateBadgeHtml = `<span style="font-size: 0.9rem; color: #cbd5e1; background-color: #1e293b; padding: 2px 8px; border-radius: 4px; border: 1px solid #334155; white-space:nowrap;">${day}${formattedMonth}/${doc.year}</span>`;
        }

        const entity = entityMap[doc.type] || { label: 'מסמך', color: '#64748b' };
        const label = doc.type === 'committee' && doc.author ? doc.author : entity.label;
        const typeBadgeHtml = `<div style="display:inline-block; background-color: ${entity.color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold; margin-bottom: 6px;">${label}</div>`;

        let sourceHtml = '';
        if (doc.type !== 'committee') {
            if (doc.source && doc.source.trim() !== '') {
                sourceHtml = `<div style="font-size:0.9rem; color:#cbd5e1; margin-bottom: 8px;"><strong>מקור:</strong> ${doc.source}</div>`;
            } else if (doc.author && doc.author.trim() !== '') {
                sourceHtml = `<div style="font-size:0.9rem; color:#cbd5e1; margin-bottom: 8px;"><strong>מחבר:</strong> ${doc.author}</div>`;
            }
        }

        if (index > 0) {
            htmlContent += `<div style="border-top: 1px solid #334155; margin: 12px 0;"></div>`;
        }

        htmlContent += `
            <div>${typeBadgeHtml}</div>
            <div style="display:flex; justify-content:space-between; align-items:flex-start; border-bottom: 1px solid #334155; padding-bottom: 8px; margin-bottom: 8px; gap: 12px;">
                <span style="font-weight: bold; color: #93c5fd; font-size: 1.15rem; line-height: 1.3;">${doc.title}</span>
                ${dateBadgeHtml}
            </div>
            ${sourceHtml}
            <div style="font-size:0.9rem; line-height:1.5;">${doc.summary}</div>
        `;
    });

    if (!htmlContent) return;
    tooltip.innerHTML = htmlContent;

    let leftPos = event.clientX + 15;
    if (leftPos + 350 > window.innerWidth) {
        leftPos = event.clientX - 365;
    }
    tooltip.style.left = leftPos + 'px';
    tooltip.style.top = (event.clientY + 15) + 'px';
    tooltip.style.opacity = '1';
}

function hideDocTooltip() {
    const tooltip = document.getElementById('docHoverTooltip');
    if(tooltip) tooltip.style.opacity = '0';
}